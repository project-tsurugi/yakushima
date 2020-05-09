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
    free(value_ptr_);
  }

  void set_value(ValueType *value, std::size_t value_length) {
    if (need_delete_value_) {
      delete value_ptr_;
    } else {
      need_delete_value_ = true;
    }
    next_layer_ = nullptr;
    try {
      value_ptr_ = new ValueType[value_length];
    } catch (std::bad_alloc& e) {
      std::cout << e.what() << std::endl;
    }
    memcpy(value_ptr_, value, value_length);
    value_length_ = value_length;
  }

private:
  base_node *next_layer_{nullptr};
  ValueType *value_ptr_{nullptr};
  std::size_t value_length_{};
  bool need_delete_value_{false};
};

} // namespace yakushima
