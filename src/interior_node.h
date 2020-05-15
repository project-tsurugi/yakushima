/**
 * @file mt_interior_node.h
 */

#pragma once

#include <cstdint>

#include "atomic_wrapper.h"
#include "base_node.h"
#include "border_node.h"

namespace yakushima {
class interior_node final : public base_node {
public:
  static constexpr std::size_t child_length = 16;
  using n_keys_body_type = std::uint8_t;
  using n_keys_type = std::atomic<n_keys_body_type>;

  interior_node() = default;

  ~interior_node() = default;

  /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   */
  void destroy() final {
    for (auto i = 0; i < n_keys_; ++i) {
      child[i]->destroy();
    }
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
        ret_child = get_child_at(i);
        /**
         * It loads key_slice atomically by get_key_slice_at func.
         */
        if (ret_child != nullptr && key_slice == get_key_slice_at(i))
          break;
        else
          ret_child = nullptr;
      }
      node_version64_body check = get_stable_version();
      if (v == check) return ret_child;
      else v = check;
    }
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
