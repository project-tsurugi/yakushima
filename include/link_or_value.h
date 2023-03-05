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
        }
        init_lv();
    }

    /**
     * @details display function for analysis and debug.
     */
    void display() const {
        if (auto* child = get_next_layer(); child != nullptr) {
            std::cout << "need_delete_value_ : " << false << std::endl;
            std::cout << "next_layer_ : " << child << std::endl;
            std::cout << "v_or_vp_ : " << nullptr << std::endl;
            std::cout << "value_length_ : " << 0 << std::endl;
            std::cout << "value_align_ : " << 0 << std::endl;
        } else if (auto* v = get_value(); v != nullptr) {
            const auto del_flag = value::need_delete(v);
            const auto v_align = static_cast<std::size_t>(
                    std::get<2>(value::get_gc_info(v)));
            std::cout << "need_delete_value_ : " << del_flag << std::endl;
            std::cout << "next_layer_ : " << nullptr << std::endl;
            std::cout << "v_or_vp_ : " << value::get_body(v) << std::endl;
            std::cout << "value_length_ : " << value::get_len(v) << std::endl;
            std::cout << "value_align_ : " << v_align << std::endl;
        }
    }

    /**
     * @brief Collect the memory usage of this record.
     * 
     * @param[in] level The level of this node in the tree.
     * @param[in,out] mem_stat The stack of memory usage for each level.
     */
    void mem_usage(std::size_t level, memory_usage_stack& mem_stat) const {
        if (auto* child = get_next_layer(); child != nullptr) {
            child->mem_usage(level + 1, mem_stat);
        } else if (auto* v = get_value(); v != nullptr) {
            const auto v_len = std::get<1>(value::get_gc_info(v));
            auto& [node_num, used, reserved] = mem_stat.at(level);
            used += v_len;
            reserved += v_len;
        }
    }

    [[maybe_unused]] [[nodiscard]] const std::type_info* get_lv_type() const {
        if (get_next_layer() != nullptr) { return &typeid(base_node*); }
        if (get_value() != nullptr) { return &typeid(value*); }
        return &typeid(nullptr);
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
        if ((ptr & kChildFlag) == 0) { return nullptr; }
        return reinterpret_cast<base_node*>(ptr & ~kChildFlag); // NOLINT
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
        if ((ptr & kChildFlag) > 0 || ptr == kValPtrFlag) { return nullptr; }
        return reinterpret_cast<value*>(ptr); // NOLINT
    }

    /**
     * @brief Initialize the payload to zero.
     * 
     */
    void init_lv() { child_or_v_ = kValPtrFlag; }

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
     * @brief todo : write documents much.
     * @param[in] new_value todo write
     * @param[out] created_value_ptr todo write
     * @param[out] old_value todo write
     */
    void set_value(value* new_value, void** const created_value_ptr,
                   value** old_value = nullptr) {
        auto* cur_v = get_value();
        if (cur_v != nullptr && value::need_delete(cur_v)) {
            if (old_value == nullptr) {
                value::delete_value(cur_v);
            } else {
                *old_value = cur_v;
            }
        }

        // store the given value
        const auto ptr = reinterpret_cast<uintptr_t>(new_value); // NOLINT
        storeReleaseN(child_or_v_, ptr);
        if (created_value_ptr != nullptr) {
            auto* v_ptr = reinterpret_cast<value*>(child_or_v_); // NOLINT
            *created_value_ptr = value::get_body(v_ptr);
        }
    }

    /**
     * @pre This function called at initialization.
     * @param[in] new_next_layer
     */
    void set_next_layer(base_node* const new_next_layer) {
        auto ptr = reinterpret_cast<uintptr_t>(new_next_layer); // NOLINT
        storeReleaseN(child_or_v_, ptr | kChildFlag);
    }

private:
    /**
     * @brief A flag for indicating that the next layer exists.
     * 
     */
    static constexpr uintptr_t kChildFlag = 0b10UL << 62UL;

    /**
     * @brief A flag for indicating that the next layer exists.
     * 
     */
    static constexpr uintptr_t kValPtrFlag = 0b01UL << 62UL;

    /**
     * @attention
     * This variable is read/write concurrently.
     * If all the bits are zeros, this does not have any data.
     * If the most significant bit is one, this contains the next layer.
     * Otherwise, this contains the pointer of a value.
     */
    uintptr_t child_or_v_{kValPtrFlag};
};

} // namespace yakushima