/**
 * @file mt_base_node.h
 */

#pragma once

#include "cpu.h"
#include "mt_interior_node.h"

namespace yakushima {

class base_node {
public:
  [[nodiscard]] uint64_t get_stable_version() & {
    return version_.get_stable_version();
  }

private:
  alignas(CACHE_LINE_SIZE)
  node_version64 version_;
  interior_node *parent;
  uint64_t key_slice[15];
};

} // namespace yakushima