/**
 * @file mt_interior_node.h
 */

#pragma once

#include <cstdint>

#include "mt_base_node.h"
#include "mt_border_node.h"

// forward declaration
class border_node;

namespace yakushima {
class interior_node : private base_node {
public:
  static constexpr std::size_t child_length = 16;

private:
  // first member of base_node is aligned along with cache line size.
  uint8_t nkeys_;
  border_node *child[child_length];
};

} // namespace yakushima
