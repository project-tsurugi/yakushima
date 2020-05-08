/**
 * @file mt_kvs.h
 * @brief This file defines the body of masstree.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "scheme.h"

namespace yakushima {

template<class ValueType>
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

  static status put([[maybe_unused]]std::string key, [[maybe_unused]]ValueType value) {
    if (root_.load(std::memory_order_acquire) == nullptr) {
      border_node<ValueType> *new_root = new border_node<ValueType>();
      new_root->set_as_root(key, value);
      for (;;) {
        if (root_.compare_exchange_strong(nullptr, new_root, std::memory_order_acq_rel, std::memory_order_acquire)) {
          return status::OK;
        } else {
          if (root_.load(std::memory_order_acquire) != nullptr) {
            delete new_root;
            break;
          }
        }
      }
    }
    /**
     * here, root is not empty.
     * process put for existing tree.
     */

    return status::OK;
  }

  static status remove([[maybe_unused]]std::string key) {
    return status::OK;
  }

private:
  /**
   * @details
   * Todo : It will be container to be able to switch database.
   */
  static std::atomic<base_node *> root_;
};

template<class ValueType>
std::atomic<base_node *> masstree_kvs<ValueType>::root_ = nullptr;

} // namespace yakushima
