/**
 * @file link_or_vlaue.h
 */

#include "cpu.h"
#include "base_node.h"

#pragma once

namespace yakushima {

class link_or_value {
public:
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
    ::operator delete(v_or_vp_);
    v_or_vp_ = nullptr;
    need_delete_value_ = false;
  }

  /**
   * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
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
        ValueType *vp_tmp = static_cast<void *>(v_or_vp_);
        for (auto i = 0; i < arg_value_size / sizeof(ValueType); ++i) {
          memcpy(&vp_tmp[i], &vptr[i], sizeof(ValueType));
        }
      }
    } catch (std::bad_alloc &e) {
      std::cout << e.what() << std::endl;
    }
  }

  void set_next_layer(base_node* next_layer) {
    next_layer_ = next_layer;
    if (need_delete_value_) {
      destroy();
    }
  }

private:
  /**
   * @attention
   * This variable is read concurrently.
   * This variable is updated at initialization and destruction.
   */
  base_node *next_layer_{nullptr};
  /**
   * @details
   * This variable is stored value body whose size is less than pointer or pointer to value.
   * This variable is read concurrently.
   */
  void *v_or_vp_{nullptr};
  /**
   * @attention
   * This variable is read concurrently.
   * This variable is updated at initialization and destruction.
   */
  std::size_t value_length_{0};
  /**
   * @attention
   * This variable is read concurrently.
   * This variable is updated at initialization and destruction.
   */
  bool need_delete_value_{false};
};

} // namespace yakushima
