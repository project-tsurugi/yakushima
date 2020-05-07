/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>

#include "interior_node.h"
#include "link_or_value.h"
#include "permutation.h"

// forward declaration
struct interior_node;

namespace yakushima {

template<class ValueType>
class border_node : private base_node {
public:
  static constexpr std::size_t node_fanout = 15;
private:
  // first member of base_node is aligned along with cache line size.
  uint8_t nremoved_{};
  uint8_t keylen[node_fanout]{};
  permutation permutation_{};
  link_or_value <ValueType> lv[node_fanout]{};
  border_node *next_{};
  /**
   * @attention This is protected by its previous sibling's lock.
   */
  border_node *prev_{};
  std::string key_suffix_{};
};
} // namespace yakushima

