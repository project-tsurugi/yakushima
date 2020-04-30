/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>

#include "mt_interior_node.h"

// forward declaration
struct interior_node;

namespace yakushima {
class border_node : private base_node {
public:
private:
  uint8_t nremoved_;
  uint8_t keylen[15]_;
  uint64_t permutation_;
  link_or_value lv[15]_;
  border_node *next_;
  border_node *prev_;
  keysuffix_t keysuffixes_;
};
} // namespace yakushima

