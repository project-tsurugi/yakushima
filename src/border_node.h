/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>

#include "interior_node.h"

// forward declaration
struct interior_node;

namespace yakushima {
class border_node : private base_node {
public:
  static constexpr std::size_t node_fanout = 15;
private:
  // first member of base_node is aligned along with cache line size.
  uint8_t nremoved_;
  uint8_t keylen[node_fanout]_;
  uint64_t permutation_;
  link_or_value lv[node_fanout]_;
  border_node *next_;
  border_node *prev_;
  keysuffix_t keysuffixes_;
};
} // namespace yakushima

