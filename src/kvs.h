/**
 * @file mt_kvs.h
 * @brief This file defines the body of masstree.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "interior_node.h"
#include "scheme.h"

namespace yakushima {

class masstree_kvs {
public:
  /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   * @return status::OK
   * @return status::ERR_UNKNOWN_ROOT The root is not both interior and border.
   */
  static status destroy() {
    base_node *local_root = root_.load(std::memory_order_acquire);
    if (typeid(*local_root) == typeid(interior_node))
      static_cast<interior_node *>(local_root)->destroy_interior();
    else if (typeid(*local_root) == typeid(border_node))
      static_cast<border_node *>(local_root)->destroy_border();
    else
      return status::ERR_UNKNOWN_ROOT;
    return status::OK;
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
  static status put(std::string_view key_view, ValueType *value,
                    std::size_t arg_value_length = sizeof(ValueType),
                    std::size_t value_align = alignof(ValueType)) {
    base_node *expected = root_.load(std::memory_order_acquire);
    if (expected == nullptr) {
      /**
       * root is nullptr, so put single border nodes.
       */
      border_node *new_root = new border_node();
      new_root->set_as_root(key_view, value, arg_value_length, value_align);
      for (;;) {
        if (root_.compare_exchange_weak(expected, dynamic_cast<base_node *>(new_root), std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
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
    bool final_slice{false};
    std::size_t slice_index{0};
retry:
    std::uint64_t key_slice{0};
    if (key_view.size() > sizeof(std::uint64_t)) {
      memcpy(&key_slice, key_view.data(), sizeof(std::uint64_t));
    } else {
      memcpy(&key_slice, key_view.data(), key_view.size());
      final_slice = true;
    }
    std::tuple<base_node *, node_version64_body> node_and_v = find_border(key_slice);

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

  static std::tuple<base_node *, node_version64_body>
  find_border(std::uint64_t key_slice) {
retry:
    base_node *n = get_root();
    node_version64_body v = n->get_stable_version();
    if (n != get_root()) goto retry;
[[maybe_unused]]descend:
    if (typeid(*n) == typeid(border_node))
      return std::make_tuple(n, v);
    /**
     * @a n points to a interior_node object.
     */
    [[maybe_unused]]base_node *n_child = static_cast<interior_node *>(n)->get_child(key_slice);
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
