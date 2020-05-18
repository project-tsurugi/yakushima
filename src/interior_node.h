/**
 * @file mt_interior_node.h
 */

#pragma once

#include <string.h>

#include <cstdint>

#include "atomic_wrapper.h"
#include "base_node.h"
#include "border_node.h"

namespace yakushima {

class interior_node final : public base_node {
public:
  /**
   * @details The structure is "ptr, key, ptr, key, ..., ptr".
   * So the child_length is key_slice_length plus 1.
   */
  static constexpr std::size_t child_length = base_node::key_slice_length + 1;
  using n_keys_body_type = std::uint8_t;
  using n_keys_type = std::atomic<n_keys_body_type>;

  interior_node() = default;

  ~interior_node() = default;

  /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   */
  status destroy() final {
    for (auto i = 0; i < n_keys_; ++i) {
      child[i]->destroy();
    }
    delete this;
    return status::OK_DESTROY_INTERIOR;
  }

  [[nodiscard]] n_keys_body_type get_n_keys() {
    return n_keys_.load(std::memory_order_acquire);
  }

  [[nodiscard]] base_node* get_child_at(std::size_t index) {
    return loadAcquire(child[index]);
  }

  base_node *get_child_of(base_node::key_slice_type key_slice) {
    node_version64_body v = get_stable_version();
    for (;;) {
      n_keys_body_type n_key = get_n_keys();
      base_node *ret_child{nullptr};
      for (auto i = 0; i < n_key; ++i) {
        /**
         * It loads key_slice atomically by get_key_slice_at func.
         */
         base_node::key_slice_type slice_at_index = get_key_slice_at(i);
        if (memcmp(&key_slice, &slice_at_index, sizeof(base_node::key_slice_type)) < 0) {
          /**
           * The key_slice must be left direction of the index.
           */
          ret_child = get_child_at(i);
          break;
        } else {
          /**
           * The key_slice must be right direction of the index.
           */
           if (i == n_key -1) {
             ret_child = get_child_at(i+1);
             break;
           }
        }
      }
      node_version64_body check = get_stable_version();
      if (v == check) return ret_child;
      else v = check;
    }
  }

  void init_interior() {
    init_base();
    set_version_border(true);
    for (std::size_t i = 0; i < child_length; ++i) {
      child[i] = nullptr;
    }
    n_keys_ = 0;
  }

private:
  /**
   * first member of base_node is aligned along with cache line size.
   */

  /**
   * @attention This variable is read/written concurrently.
   */
  base_node *child[child_length]{};
  /**
   * @attention This variable is read/written concurrently.
   */
  n_keys_type n_keys_{};
};

} // namespace yakushima
