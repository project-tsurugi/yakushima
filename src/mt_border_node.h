/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>

#include "mt_interior_node.h"

// forward declaration
struct interior_node;

namespace yakushima {
struct border_node {
public:
private:
  uint32_t version;
  uint8_t nremoved;
  uint8_t keylen[15];
  uint64_t permutation;
  uint64_t keyslice[15];
  link_or_value lv[15];
  border_node *next;
  border_node *prev;
  interior_node* parent;
  keysuffix_t keysuffixes;
};
} // namespace yakushima

