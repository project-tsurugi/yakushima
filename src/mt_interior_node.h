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
  uint8_t nkeys_;
  _;
  border_node *child[16]
  _;
};

} // namespace yakushima
