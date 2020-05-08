/**
 * @file mt_kvs.h
 * @brief This file defines the body of masstree.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "scheme.h"

namespace yakushima {

class masstree_kvs {
public:
  /**
   * @brief release all heap objects and clean up.
   */
  static void destroy() {
    root_.load(std::memory_order_acquire)->destroy();
  }

  /**
   * @param key [in] The key.
   * @param res [out] The result status.
   * @return ValueType*
   */
  template<class ValueType>
  [[nodiscard]] static ValueType *get([[maybe_unused]]std::string key, [[maybe_unused]]status &res) {
    res = status::OK;
    return nullptr;
  }

  static base_node *get_root() {
    return root_.load(std::memory_order_acquire);
  }

  static void init_kvs() {
    root_.store(nullptr, std::memory_order_release);
  }

  template<class ValueType>
  static status put([[maybe_unused]]std::string key, [[maybe_unused]]ValueType value) {
    if (root_.load(std::memory_order_acquire) == nullptr) {
    }
    return status::OK;
  }

  static status remove([[maybe_unused]]std::string key) {
    return status::OK;
  }

private:
  /**
   * @pre This function is called only when root is nullptr.
   * @return status::OK It created empty border node correctly.
   * @return status::WARN_BAD_USAGE Root is not nullptr.
   */
  static status create_empty_border_node_as_root() {
    base_node *root(root_.load(std::memory_order_acquire));
    for (;;) {
      if (root == nullptr) return status::WARN_BAD_USAGE;
    }
  }

  /**
   * @details
   * Todo : It will be container to be able to switch database.
   */
  static std::atomic<base_node *> root_;
  static bool end_init_kvs_;
};

} // namespace yakushima
