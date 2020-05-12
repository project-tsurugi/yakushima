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
  void destroy() {
    delete v_or_vp_;
    v_or_vp_ = nullptr;
    need_delete_value_ = false;
  }

  void set_value(ValueType *value, std::size_t value_length) {
    if (need_delete_value_) {
      delete v_or_vp_;
    }
    next_layer_ = nullptr;
    try {
      /**
       * @details It use copy assign, so ValueType must be copy-assignable.
       */
       if (sizeof(ValueType) == value_length) {
         v_or_vp_ = ::operator new(sizeof(ValueType), static_cast<std::align_val_t>(alignof(std::max_align_t)));
         *static_cast<ValueType>(v_or_vp_) = *value;
       } else {
         /**
          * Value is variable-length.
          */
         v_or_vp_ = ::operator new(sizeof(value_length), static_cast<std::align_val_t>(alignof(std::max_align_t)));
         ValueType* vp = value;
         for (auto i = 0; i < value_length / sizeof(ValueType); ++i) {
           vp[i] = value[i];
         }
       }

      need_delete_value_ = true;
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
  alignas(std::max_align_t) char v_or_vp_[sizeof(ValueType*)];
  std::size_t value_length;
  bool need_delete_value_{false};
};

} // namespace yakushima
