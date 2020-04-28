/**
 * @file mt_interior_node.h
 */

#pragma once

#include <cstdint>

#include "mt_border_node.h"

// forward declaration
struct border_node;

namespace yakushima {
struct interior_node {
public:
private:
  uint32_t version;
  uint8_t nkeys;
  uint64_t keyslice[15];
  border_node *child[16];
  interior_node *parent;
};

} // namespace yakushima
