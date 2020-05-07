/**
 * @file mt_base_node.h
 */

#pragma once

#include "cpu.h"
#include "version.h"

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

  /**
   * @brief It locks this node.
   * @pre It didn't lock by myself.
   * @return void
   */
  void lock() & {
    version_.lock();
  }

  /**
   * @pre This function is called by split.
   */
  base_node* lock_parent()  & {
    for (;;) {
      base_node* p = parent_.load(std::memory_order_acquire);
      if (p == nullptr) return p;
       p->lock();
      if (p == parent_.load(std::memory_order_acquire)) {
        return p;
      } else {
        p->unlock();
      }
    }
  }

  /**
   * @brief It unlocks this node.
   * @pre This node was already locked.
   * @return void
   */
  void unlock() & {
    version_.unlock();
  }

private:
  alignas(CACHE_LINE_SIZE)
  node_version64 version_{};
  /**
   * @details Member parent_ is a type "std::atomic<interior_node*>" essentially,
   * but declare a type "std::atomic<base_node*>" due to including file order.
   * @attention This member is protected by its parent's lock.
   */
  std::atomic<base_node*> parent_{nullptr};
  uint64_t key_slice_[key_slice_length]{};
};

} // namespace yakushima
