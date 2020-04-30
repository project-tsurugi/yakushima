/**
 * @file link_or_vlaue.h
 */

#pragma once

template <class NODE>
class link_or_value {
public:
  union {
    NODE* next_layer_;
    char* value_;
  };

private:
  std::size_t value_length;
};