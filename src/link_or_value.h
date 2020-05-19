#pragma once

/**
 * @file link_or_vlaue.h
 */

#include "atomic_wrapper.h"
#include "base_node.h"
#include "cpu.h"

#include <typeinfo>

namespace yakushima {

class link_or_value {
public:
  using value_size_type = std::size_t;

  link_or_value() = default;

  link_or_value(const link_or_value &) = default;

  link_or_value(link_or_value &&) = default;

  link_or_value &operator=(const link_or_value &) = default;

  link_or_value &operator=(link_or_value &&) = default;

  ~link_or_value() = default;

  /**
   * @details release heap objects.
   */
  void destroy() {
    /**
     * about next layer
     */
     if (next_layer_ != nullptr) {
       next_layer_->destroy();
     }
    next_layer_ = nullptr;
    /**
     * about value
     */
    if (get_need_delete_value()) {
      ::operator delete(v_or_vp_);
    }
    set_need_delete_value(false);
    set_v_or_vp(nullptr);
    set_value_length(0);
  }

  [[nodiscard]] bool get_need_delete_value() {
    return loadAcquireN(need_delete_value_);
  }

  [[nodiscard]] base_node *get_next_layer() {
    return loadAcquireN(next_layer_);
  }

  [[nodiscard]] const std::type_info *get_lv_type() {
    if (get_next_layer() != nullptr) {
      return &typeid(get_next_layer());
    } else if (get_v_or_vp_() != nullptr) {
      return &typeid(get_v_or_vp_());
    } else {
      return &typeid(nullptr);
    }
  }

  [[nodiscard]] void *get_v_or_vp_() {
    return loadAcquireN(v_or_vp_);
  }

  void init_lv() {
    set_need_delete_value(false);
    set_next_layer(nullptr);
    set_v_or_vp(nullptr);
    set_value_length(0);
  }

  /**
   * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
   * @pre This function called at initialization.
   * @param vptr The pointer to source value object.
   * @param arg_value_size The byte size of value.
   * @param value_align The alignment of value.
   */
  template<class ValueType>
  void set_value(ValueType *vptr, std::size_t arg_value_size = sizeof(ValueType),
                 std::size_t value_align = alignof(ValueType)) {
    if (need_delete_value_) {
      ::operator delete(v_or_vp_);
      need_delete_value_ = false;
    }
    next_layer_ = nullptr;
    value_length_ = arg_value_size;
    try {
      /**
       * @details It use copy assign, so ValueType must be copy-assignable.
       */
      if (sizeof(ValueType) == arg_value_size) {
        if (arg_value_size <= sizeof(void *)) {
          memcpy(&v_or_vp_, vptr, arg_value_size);
          need_delete_value_ = false;
        } else {
          v_or_vp_ = ::operator new(sizeof(ValueType),
                                    static_cast<std::align_val_t>(value_align));
          memcpy(v_or_vp_, vptr, arg_value_size);
          need_delete_value_ = true;
        }
      } else {
        /**
         * Value is variable-length.
         */
        v_or_vp_ = ::operator new(sizeof(arg_value_size),
                                  static_cast<std::align_val_t>(value_align));
        need_delete_value_ = true;
        ValueType *vp_tmp = static_cast<ValueType*>(v_or_vp_);
        for (std::size_t i = 0; i < arg_value_size / sizeof(ValueType); ++i) {
          memcpy(&vp_tmp[i], &vptr[i], sizeof(ValueType));
        }
      }
    } catch (std::bad_alloc &e) {
      std::cout << e.what() << std::endl;
    }
  }

  /**
   * @pre This function called at initialization.
   * @param [in] next_layer
   */
  void set_next_layer(base_node *new_next_layer) {
    storeReleaseN(next_layer_, new_next_layer);
  }

  void set_v_or_vp(void *new_v_or_vp) {
    storeReleaseN(v_or_vp_, new_v_or_vp);
  }

  void set_value_length(value_size_type new_value_length) {
    storeReleaseN(value_length_, new_value_length);
  }

  void set_need_delete_value(bool new_need_delete_value) {
    storeReleaseN(need_delete_value_, new_need_delete_value);
  }

private:
  /**
   * @attention
   * This variable is read/write concurrently.
   * If this is nullptr, value is stored.
   * If this is not nullptr, it contains next_layer.
   */
  base_node *next_layer_{nullptr};
  /**
   * @details
   * This variable is stored value body whose size is less than pointer or pointer to value.
   * This variable is read/write concurrently.
   */
  void *v_or_vp_{nullptr};
  /**
   * @attention
   * This variable is read/write concurrently.
   * This variable is updated at initialization and destruction.
   */
  value_size_type value_length_{0};
  /**
   * @attention
   * This variable is read/write concurrently.
   * This variable is updated at initialization and destruction.
   */
  bool need_delete_value_{false};
};

} // namespace yakushima
