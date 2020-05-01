/**
 * @file mt_base_node.h
 */

#pragma once

#include "cpu.h"
#include "mt_version.h"

namespace yakushima {

class base_node {
public:
  static constexpr std::size_t key_slice_length = 15;

  base_node() : parent_{nullptr} , key_slice_{} {
    version_.init();
    parent_.store(nullptr, std::memory_order_release);
  }

  [[nodiscard]] uint64_t get_key_slice_at(std::size_t index) {
    if (index > static_cast<std::size_t>(key_slice_length)) {
      std::abort();
    }
    return key_slice_[index];
  }

  [[nodiscard]] node_version64_body get_stable_version() & {
    return version_.get_stable_version();
  }

  [[nodiscard]] base_node* get_parent() & {
    return parent_.load(std::memory_order_acquire);
  }

  [[nodiscard]] node_version64 get_version() & {
    return version_;
  }

private:
  alignas(CACHE_LINE_SIZE)
  node_version64 version_;
  /**
   * @details Member parent_ is a type "std::atomic<interior_node*>" essentially,
   * but declare a type "std::atomic<base_node*>" due to including file order.
   */
  std::atomic<base_node*> parent_;
  uint64_t key_slice_[key_slice_length];
};

} // namespace yakushima