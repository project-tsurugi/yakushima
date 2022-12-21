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
        if (get_need_delete_value()) {
            ::operator delete(get_v_or_vp_(), static_cast<std::align_val_t>(
                                                      (get_value_align())));
        }
        set_need_delete(false);
        set_v_or_vp(nullptr);
        set_value_length(0);
        set_value_align(static_cast<std::align_val_t>(0));
    }

    /**
   * @details display function for analysis and debug.
   */
    void display() const {
        std::cout << "need_delete_value_ : " << get_need_delete_value()
                  << std::endl;
        std::cout << "next_layer_ : " << get_next_layer() << std::endl;
        std::cout << "v_or_vp_ : " << get_v_or_vp_() << std::endl;
        std::cout << "value_length_ : " << get_value_length() << std::endl;
        std::cout << "value_align_ : "
                  << static_cast<std::size_t>(get_value_align()) << std::endl;
    }

    [[nodiscard]] bool get_need_delete_value() const { return need_delete_; }

    [[nodiscard]] base_node* get_next_layer() const {
        return loadAcquireN(next_layer_);
    }

    [[maybe_unused]] [[nodiscard]] const std::type_info* get_lv_type() const {
        if (get_next_layer() != nullptr) { return &typeid(get_next_layer()); }
        if (get_v_or_vp_() != nullptr) { return &typeid(get_v_or_vp_()); }
        return &typeid(nullptr);
    }

    [[nodiscard]] void* get_v_or_vp_() const { return value_.get_body(); }

    [[nodiscard]] value_align_type get_value_align() const {
        return value_.get_align();
    }

    [[nodiscard]] value_length_type get_value_length() const {
        return value_.get_len();
    }

    void init_lv() {
        set_need_delete(false);
        set_next_layer(nullptr);
        set_v_or_vp(nullptr);
        set_value_length(0);
        set_value_align(static_cast<std::align_val_t>(0));
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
    void set_value(value const new_value, void** const created_value_ptr) {
        if (get_need_delete_value()) {
            ::operator delete(get_v_or_vp_(), get_value_length(),
                              get_value_align());
            set_need_delete(false);
        }
        set_next_layer(nullptr);
        set_value_length(new_value.get_len());
        set_value_align(new_value.get_align());
        try {
            /**
       * It use copy assign, so ValueType must be copy-assignable.
       */
            // todo inline 8 bytes value.
            set_v_or_vp(::operator new(
                    new_value.get_len(),
                    static_cast<std::align_val_t>(new_value.get_align())));
            if (created_value_ptr != nullptr) {
                *created_value_ptr = get_v_or_vp_();
            }
            memcpy(get_v_or_vp_(), new_value.get_body(), new_value.get_len());
            set_need_delete(true);
        } catch (std::bad_alloc& e) {
            LOG(ERROR) << log_location_prefix << e.what();
        }
    }

    /**
   * @brief for update operation. todo : write documents much.
   * @param[in] new_value todo write
   * @param[out] old_value todo write
   * @param[out] created_value_ptr todo write
   */
    void set_value(value const new_value, value& old_value,
                   void** const created_value_ptr) {
        if (get_need_delete_value()) {
            old_value = value_;
            set_need_delete(false);
        } else {
            old_value = value{};
        }
        set_next_layer(nullptr);
        set_value_length(new_value.get_len());
        set_value_align(new_value.get_align());
        try {
            /**
       * It use copy assign, so ValueType must be copy-assignable.
       */
            set_v_or_vp(::operator new(
                    new_value.get_len(),
                    static_cast<std::align_val_t>(new_value.get_align())));
            if (created_value_ptr != nullptr) {
                *created_value_ptr = get_v_or_vp_();
            }
            memcpy(get_v_or_vp_(), new_value.get_body(), new_value.get_len());
            set_need_delete(true);
        } catch (std::bad_alloc& e) {
            LOG(ERROR) << log_location_prefix << e.what();
        }
    }

    /**
   * @pre This function called at initialization.
   * @param[in] new_next_layer
   */
    void set_next_layer(base_node* const new_next_layer) {
        storeReleaseN(next_layer_, new_next_layer);
    }

    void set_v_or_vp(void* const v) { value_.set_body(v); }

    void set_value_align(const value_align_type align) {
        value_.set_align(align);
    }

    void set_value_length(const value_length_type len) { value_.set_len(len); }

    void set_need_delete(const bool tf) { need_delete_ = tf; }

private:
    /**
   * @attention
   * This variable is read/write concurrently.
   * If this is nullptr, value is stored.
   * If this is not nullptr, it contains next_layer.
   */
    base_node* next_layer_{nullptr};

    value value_{};

    /**
   * @attention
   * This variable is read/write concurrently.
   * This variable is updated at initialization and destruction.
   */
    bool need_delete_{false};
};

} // namespace yakushima