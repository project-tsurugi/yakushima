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
    delete value_ptr_;
    value_ptr_ = nullptr;
    need_delete_value_ = false;
  }

  void set_value(ValueType &value) {
    if (need_delete_value_) {
      delete value_ptr_;
    }
    next_layer_ = nullptr;
    try {
      /**
       * @details Use copy constructor, so ValueType must be copy-constructable.
       */
      value_ptr_ = ::new ValueType(value);
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
  ValueType *value_ptr_{nullptr};
  bool need_delete_value_{false};
};

} // namespace yakushima
