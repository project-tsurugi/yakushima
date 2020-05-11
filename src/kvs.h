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
  [[nodiscard]] static ValueType &get_ref([[maybe_unused]]std::string key, [[maybe_unused]]status &res) {
    res = status::OK;
    return nullptr;
  }

  static void init_kvs() {
    root_.store(nullptr, std::memory_order_release);
  }

  static status put([[maybe_unused]]std::string key, [[maybe_unused]]ValueType &value) {
    base_node* root = root_.load(std::memory_order_acquire);
    if (root == nullptr) {
      /**
       * root is nullptr, so put single border nodes.
       */
      border_node<ValueType> *new_root = new border_node<ValueType>();
      new_root->set_as_root(key, value);
      for (;;) {
        if (root_.compare_exchange_weak(root, new_root, std::memory_order_acq_rel, std::memory_order_acquire)) {
          return status::OK;
        } else {
          if (root != nullptr) {
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

  static std::tuple<base_node *, node_version64_body> find_border(std::string key) {
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

template<class ValueType>
std::atomic<base_node *> masstree_kvs<ValueType>::root_ = nullptr;

} // namespace yakushima
