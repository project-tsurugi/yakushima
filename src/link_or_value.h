/**
 * @file link_or_vlaue.h
 */

#include "cpu.h"
#include "base_node.h"

#pragma once

namespace yakushima {

template<class ValueType>
class link_or_value {
public:
  link_or_value() = default;
  link_or_value(const link_or_value<ValueType>&) = default;
  link_or_value(link_or_value<ValueType>&&) = default;
  link_or_value<ValueType>& operator=(const link_or_value&) = default;
  link_or_value<ValueType>& operator=(link_or_value<ValueType>&&) = default;
  ~link_or_value() = default;

  /**
   * @details release heap objects.
   */
  void destroy() {
    delete static_cast<ValueType>(v_or_vp_);
    v_or_vp_ = nullptr;
    need_delete_value_ = false;
  }

  /**
   * @pre @a arg_value_length is divisible by sizeof(ValueType).
   * @param value The pointer to source value object.
   * @param arg_value_length The byte size of value.
   */
  void set_value(ValueType *value, std::size_t arg_value_length) {
    if (need_delete_value_) {
      delete static_cast<ValueType>(v_or_vp_);
      need_delete_value_ = false;
    }
    next_layer_ = nullptr;
    value_length_ = arg_value_length;
    try {
      /**
       * @details It use copy assign, so ValueType must be copy-assignable.
       */
       if (sizeof(ValueType) == arg_value_length) {
         if (arg_value_length <= sizeof(void*)) {
           memcpy(v_or_vp_, value, arg_value_length);
           need_delete_value_ = false;
         } else {
           v_or_vp_ = ::operator new(sizeof(ValueType), static_cast<std::align_val_t>(alignof(ValueType)));
           *static_cast<ValueType>(v_or_vp_) = *value;
           need_delete_value_ = true;
         }
       } else {
         /**
          * Value is variable-length.
          */
         v_or_vp_ = ::operator new(sizeof(arg_value_length), static_cast<std::align_val_t>(alignof(ValueType)));
         need_delete_value_ = true;
         ValueType* vp = static_cast<ValueType*>(v_or_vp_);
         for (auto i = 0; i < arg_value_length / sizeof(ValueType); ++i) {
           memcpy(&vp[i], &value[i], sizeof(ValueType));
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
  base_node *next_layer_{nullptr};
  /**
   * @details This variable is stored value body whose size is less than pointer or pointer to value.
   */
  void* v_or_vp_{nullptr};
  std::size_t value_length_{0};
  bool need_delete_value_{false};
};

} // namespace yakushima
