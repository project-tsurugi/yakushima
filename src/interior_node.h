/**
 * @file mt_interior_node.h
 */

#pragma once

#include <cstdint>

#include "base_node.h"
#include "border_node.h"

// forward declaration
class border_node;

namespace yakushima {
class interior_node : private base_node {
public:
  static constexpr std::size_t child_length = 16;

  interior_node() = default;

  ~interior_node() = default;

  /**
   * @brief release all heap objects and clean up.
   */
  void destroy() override {
    /**
     * todo : Call destroy of all children.
     */
  }

private:
  // first member of base_node is aligned along with cache line size.
  uint8_t nkeys_;
  border_node *child[child_length];
};

} // namespace yakushima
