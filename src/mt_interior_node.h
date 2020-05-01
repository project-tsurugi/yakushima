/**
 * @file mt_interior_node.h
 */

#pragma once

#include <cstdint>

#include "mt_border_node.h"

// forward declaration
struct border_node;

namespace yakushima {
class interior_node : private base_node {
private:
  // first member of base_node is aligned along with cache line size.
  uint8_t nkeys_;
  border_node *child[16]
};

} // namespace yakushima
