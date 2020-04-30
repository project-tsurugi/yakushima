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
  [[nodiscard]] uint64_t get_stable_version() & {
    return version_.get_stable_version();
  }

private:
  node_version64 version_;
  uint8_t nremoved_;
  uint8_t keylen[15]_;
  uint64_t permutation_;
  uint64_t keyslice[15]_;
  link_or_value lv[15]_;
  border_node *next_;
  border_node *prev_;
  interior_node* parent_;
  keysuffix_t keysuffixes_;
};
} // namespace yakushima

