/**
 * @file link_or_vlaue.h
 */

#pragma once

template <class Node, class ValueType>
class link_or_value {
public:
private:
  Node* next_layer_;
  ValueType* value_ptr_;
  std::size_t value_length_;
};