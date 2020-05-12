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
   * @return ValueType*
   */
  template<class ValueType>
  [[nodiscard]] static std::tuple<ValueType *, std::size_t>
  get([[maybe_unused]]std::string key) {
    return std::make_tuple(nullptr, 0);
  }

  static void init_kvs() {
    root_.store(nullptr, std::memory_order_release);
  }

  template<class ValueType>
  static status put(std::string key, ValueType *value,
                    std::size_t arg_value_length = sizeof(ValueType),
                    std::size_t value_align = alignof(ValueType)) {
    base_node *expected = root_.load(std::memory_order_acquire);
    if (expected == nullptr) {
      /**
       * root is nullptr, so put single border nodes.
       */
      border_node *new_root = new border_node();
      new_root->set_as_root(key, value, arg_value_length, value_align);
      for (;;) {
        if (root_.compare_exchange_weak(expected, new_root, std::memory_order_acq_rel, std::memory_order_acquire)) {
          return status::OK;
        } else {
          if (expected != nullptr) {
            // root is not nullptr;
            delete new_root;
            break;
          }
          // root is nullptr.
        }
      }
    }
    /**
     * here, root is not nullptr.
     * process put for existing tree.
     */
    std::tuple<base_node *, node_version64_body> node_and_v = find_border(key);

    return status::OK;
  }

  static status remove([[maybe_unused]]std::string key) {
    return status::OK;
  }

protected:
private:
  static base_node *get_root() {
    return root_.load(std::memory_order_acquire);
  }

  static std::tuple<base_node *, node_version64_body> find_border([[maybe_unused]]std::string key) {
#if 0
    retry:
    base_node* n = get_root();
    node_version64_body v = n->get_stable_version();
    if (n != get_root()) goto retry;
    descend:
    if (typeid(n) == typeid(border_node<ValueType>*))
#endif
      node_version64_body empty;
      return std::make_tuple(nullptr, empty);
  }

  /**
   * @details
   * Todo : It will be container to be able to switch database.
   */
  static std::atomic<base_node *> root_;
};

std::atomic<base_node *> masstree_kvs::root_ = nullptr;

} // namespace yakushima
