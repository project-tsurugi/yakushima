/**
 * @file link_or_vlaue.h
 */

#include "base_node.h"

#pragma once

namespace yakushima {

template <class ValueType>
class link_or_value {
public:
private:
  base_node* next_layer_{};
  ValueType* value_ptr_{};
  std::size_t value_length_{};
};

} // namespace yakushima
