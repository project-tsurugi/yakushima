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
        destroy_next_layer();
        destroy_value();
    }

    void destroy_next_layer() {
        if (next_layer_ != nullptr) {
            next_layer_->destroy();
            delete next_layer_; // NOLINT
        }
        set_next_layer(nullptr);
    }

    void destroy_value() {
        if (get_need_delete_value()) { value::delete_value(value_); }
        value_ = nullptr;
    }

    /**
     * @details display function for analysis and debug.
     */
    void display() const {
        std::cout << "need_delete_value_ : " << get_need_delete_value()
                  << std::endl;
        std::cout << "next_layer_ : " << get_next_layer() << std::endl;
        if (value_ == nullptr) {
            std::cout << "v_or_vp_ : " << nullptr << std::endl;
            std::cout << "value_length_ : " << 0 << std::endl;
            std::cout << "value_align_ : " << 0 << std::endl;

        } else {
            std::cout << "v_or_vp_ : " << value_->get_body() << std::endl;
            std::cout << "value_length_ : " << value_->get_len() << std::endl;
            std::cout << "value_align_ : "
                      << static_cast<std::size_t>(value_->get_align())
                      << std::endl;
        }
    }

    /**
     * @brief Collect the memory usage of this record.
     * 
     * @param level the level of this node in the tree.
     * @param mem_stat the stack of memory usage for each level.
     */
    void mem_usage(std::size_t level, memory_usage_stack& mem_stat) const {
        const auto v_len = (value_ == nullptr) ? 0 : value_->get_total_len();
        auto& [node_num, used, reserved] = mem_stat.at(level);
        used += v_len;
        reserved += v_len;

        const auto* next = get_next_layer();
        if (next != nullptr) { next->mem_usage(level + 1, mem_stat); }
    }


    [[nodiscard]] bool get_need_delete_value() const {
        return (value_ == nullptr) ? false : value_->get_need_delete_value();
    }

    [[nodiscard]] base_node* get_next_layer() const {
        return loadAcquireN(next_layer_);
    }

    [[maybe_unused]] [[nodiscard]] const std::type_info* get_lv_type() const {
        if (get_next_layer() != nullptr) { return &typeid(get_next_layer()); }
        if (get_value() != nullptr) { return &typeid(get_value()); }
        return &typeid(nullptr);
    }

    [[nodiscard]] value* get_value() const { return value_; }

    void init_lv() {
        set_next_layer(nullptr);
        value_ = nullptr;
    }

    /**
     * @pre This is called by split process.
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
     * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
     * @pre This function called at initialization.
     * @param[in] new_value todo write
     * @param[out] created_value_ptr The pointer to created value in yakushima.
     */
    void set_value(value* new_value, void** const created_value_ptr) {
        // delete the old value if exist
        if (get_need_delete_value()) { value::delete_value(value_); }

        // remove the child pointer explicitly
        set_next_layer(nullptr);

        // copy the given value
        value_ = new_value;
        if (created_value_ptr != nullptr) {
            *created_value_ptr = value_->get_body();
        }
    }

    /**
     * @brief for update operation. todo : write documents much.
     * @param[in] new_value todo write
     * @param[out] old_value todo write
     * @param[out] created_value_ptr todo write
     */
    void set_value(value* new_value, value*& old_value,
                   void** const created_value_ptr) {
        old_value = get_need_delete_value() ? value_ : nullptr;

        // remove the child pointer explicitly
        set_next_layer(nullptr);

        // copy the given value
        value_ = new_value;
        if (created_value_ptr != nullptr) {
            *created_value_ptr = value_->get_body();
        }
    }

    /**
     * @pre This function called at initialization.
     * @param[in] new_next_layer
     */
    void set_next_layer(base_node* const new_next_layer) {
        storeReleaseN(next_layer_, new_next_layer);
    }

private:
    /**
     * @attention
     * This variable is read/write concurrently.
     * If this is nullptr, value is stored.
     * If this is not nullptr, it contains next_layer.
     */
    base_node* next_layer_{nullptr};

    value* value_{nullptr};
};

} // namespace yakushima