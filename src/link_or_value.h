/**
 * @file link_or_vlaue.h
 */

#pragma once

template <class NODE>
class link_or_value {
public:
  union {
    NODE* next_layer;
    char* value;
  };
private:
};