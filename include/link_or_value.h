/**
 * @file link_or_value.h
 */

#pragma once

#include "atomic_wrapper.h"
#include "base_node.h"
#include "cpu.h"
#include "log.h"
#include "value.h"

#include <new>
#include <typeinfo>

#include "glog/logging.h"

namespace yakushima {

class link_or_value {
public:
    // 00: inlined-value
    // 01: value struct
    // 10: child node -> suffix struct
    // 11: child node
    enum tag : uintptr_t {
        LeafTagBits = 0b11UL << 62U, // mask bits

        /// @brief Tag for indicating that the raw pointer value is stored (i.e. inlined).
        InlinedValue = 0b00UL << 62U,
        /// @brief Tag for indicating that the value pointer is actually a pointer (i.e. not inlined).
        ValuePtr     = 0b01UL << 62U,
        /// @brief Tag for indicating that the key suffix and value.
        SuffixValue  = 0b10UL << 62U,
        /// @brief Tag for indicating that the next layer exists.
        Child        = 0b11UL << 62U,
    };
    link_or_value() = default;

    link_or_value(const link_or_value&) = default;

    link_or_value(link_or_value&&) = default;

    link_or_value& operator=(const link_or_value&) = default;

    link_or_value& operator=(link_or_value&&) = default;

    ~link_or_value() = default;

    /**
     * @details release heap objects.
     */
    void destroy() {
        if (auto* child = get_next_layer(); child != nullptr) {
            child->destroy();
            delete child; // NOLINT
        } else if (auto* v = get_value(); v != nullptr) {
            if (value::need_delete(v)) { value::delete_value(v); }
            if (auto* suf = get_suffix(); suf != nullptr) {
                auto [p, sz, align] = suf->get_gc_info();
                ::operator delete(p, sz, align);
            }
        }
        init_lv();
    }

    /**
     * @details display function for analysis and debug.
     */
    void display() const {
        if (auto* child = get_next_layer(); child != nullptr) {
            std::cout << "need_delete_value_ : " << false << "\n"
                         "next_layer_ : " << child << "\n"
                         "v_or_vp_ : " << nullptr << "\n"
                         "value_length_ : " << 0 << "\n"
                         "value_align_ : " << 0 << "\n";
        } else if (auto* v = get_value(); v != nullptr) {
            const auto del_flag = value::need_delete(v);
            const auto v_align = static_cast<std::size_t>(
                    std::get<2>(value::get_gc_info(v)));
            std::cout << "need_delete_value_ : " << del_flag << "\n"
                         "next_layer_ : " << nullptr << "\n"
                         "v_or_vp_ : " << value::get_body(v) << "\n"
                         "value_length_ : " << value::get_len(v) << "\n"
                         "value_align_ : " << v_align << "\n";
        }
    }

    /**
     * @brief Collect the memory usage of this record.
     *
     * @param[in] layer_level The level of this B+Tree layer in the tree.
     * @param[in,out] mem_stat The stack of memory usage for each B+Tree layer level.
     */
    void mem_usage(std::size_t layer_level, memory_usage_stack& mem_stat) const {
        if (auto* child = get_next_layer(); child != nullptr) {
            child->mem_usage(layer_level + 1, 0, mem_stat);
        } else if (auto* v = get_value(); v != nullptr) {
            mem_usage_layer_stat& ls = mem_stat.at(layer_level);
            if (auto* suf = get_suffix(); suf != nullptr) {
                auto len = std::get<1>(suf->get_gc_info());
                ls.sv_count++;
                ls.sv_allocated_mem += len;
                v = suf->get_value();
            }
            if (value::is_value_ptr(v)) {
                const auto v_len = std::get<1>(value::get_gc_info(v));
                ls.vv_count++;
                ls.vv_allocated_mem += v_len;
            } else {
                ls.iv_count++;
            }
        }
    }

    [[maybe_unused]] [[nodiscard]] const std::type_info* get_lv_type() const {
        if (get_next_layer() != nullptr) { return &typeid(base_node*); }
        if (get_value() != nullptr) { return &typeid(value*); }
        return &typeid(nullptr);
    }

    [[nodiscard]] tag get_lv_typetag() const {
        const auto ptr = loadAcquireN(child_or_v_);
        return static_cast<tag>(ptr & tag::LeafTagBits);
    }

    /**
     * @brief Get the root node of the next layer.
     *
     * Note that this function uses the atomic operation (i.e., load) for dealing with
     * concurrent modifications.
     *
     * @retval The root node of the next layer if exists.
     * @retval nullptr otherwise.
     */
    [[nodiscard]] base_node* get_next_layer() const {
        const auto ptr = loadAcquireN(child_or_v_);
        if ((ptr & tag::LeafTagBits) != tag::Child) { return nullptr; }
        return reinterpret_cast<base_node*>(ptr & ~tag::LeafTagBits); // NOLINT
    }

    /**
     * @brief Get the value pointer.
     *
     * Note that this function uses the atomic operation (i.e., load) for dealing with
     * concurrent modifications.
     *
     * @retval The pointer of the contained value if exists.
     * @retval nullptr otherwise.
     */
    [[nodiscard]] value* get_value() const {
        const auto ptr = loadAcquireN(child_or_v_);
        if ((ptr & tag::LeafTagBits) == tag::Child || ptr == tag::ValuePtr) { return nullptr; }
        if ((ptr & tag::LeafTagBits) == tag::SuffixValue) {
            return reinterpret_cast<lv_suffix*>(ptr & ~tag::LeafTagBits)->get_value(); // NOLINT
        }
        return reinterpret_cast<value*>(ptr); // NOLINT
    }

    [[nodiscard]] lv_suffix* get_suffix() const {
        const auto ptr = loadAcquireN(child_or_v_);
        if ((ptr & tag::LeafTagBits) != tag::SuffixValue) { return nullptr; }
        return reinterpret_cast<lv_suffix*>(ptr & ~tag::LeafTagBits); // NOLINT
    }

    /**
     * @brief Initialize the payload to zero.
     *
     */
    void init_lv() { child_or_v_ = tag::ValuePtr; }

    /**
     * @details This is move process.
     * @param nlv
     */
    void set(link_or_value* const nlv) {
        /**
         * This object in this function is not accessed concurrently, so it can copy assign.
         */
        *this = *nlv;
    }

    /**
     * @pre acquire the lock of the border node to which @a this lv belongs
     * @brief set the new value and release the old value if needed
     * @param[in] new_value the new value to be set
     * @param[out] created_value_ptr output parameter filled with the created value pointer
     * @param[out] old_value output parameter filled with the old value pointer. If caller receives non-null pointer,
     * it transfers ownership and caller is responsible for deleting the old value pointer.
     * This parameter must only be set where this lv is alive (i.e. from overwrite process in put()).
     * If not, the object pointed by lv has already been handed over the GC and cannot be safely accessed.
     */
    void set_value(value* new_value, void** const created_value_ptr,
                   value** old_value = nullptr) {
        if (old_value != nullptr) {
            auto* cur_v = get_value();
            if (auto* suf = get_suffix(); suf != nullptr) {
                cur_v = suf->get_value();
            }
            *old_value = cur_v;
        }

        // store the given value
        const auto ptr = reinterpret_cast<uintptr_t>(new_value); // NOLINT
        storeReleaseN(child_or_v_, ptr);
        if (created_value_ptr != nullptr) {
            auto* v_ptr = get_value();
            *created_value_ptr = value::get_body(v_ptr);
        }
    }

    /**
     * @pre This function called at initialization.
     * @param[in] new_next_layer
     */
    void set_next_layer(base_node* const new_next_layer) {
        auto ptr = reinterpret_cast<uintptr_t>(new_next_layer); // NOLINT
        storeReleaseN(child_or_v_, ptr | tag::Child);
    }

private:

    /**
     * @attention
     * This variable is read/write concurrently.
     * If all the bits are zeros, this does not have any data.
     * If the most significant bit is one, this contains the next layer.
     * Otherwise, this contains the pointer of a value.
     */
    uintptr_t child_or_v_{tag::ValuePtr};
};

} // namespace yakushima
