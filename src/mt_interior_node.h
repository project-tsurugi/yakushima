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
  node_version64 version_;
  uint8_t nkeys_;
  uint64_t keyslice[15]_;
  border_node *child[16]_;
  interior_node *parent_;
};

} // namespace yakushima
