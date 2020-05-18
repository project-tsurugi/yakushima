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
   */
  static status destroy() {
    root_.load(std::memory_order_acquire)->destroy();
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
      border_node *new_border = new border_node();
      new_border->set(key_view, value, true, arg_value_length, value_align);
      for (;;) {
        if (root_.compare_exchange_weak(expected, dynamic_cast<base_node *>(new_border), std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
          return status::OK;
        } else {
          if (expected != nullptr) {
            // root is not nullptr;
            new_border->destroy();
            delete new_border;
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
    std::string_view traverse_key_view{key_view};
retry:
    base_node::key_slice_type key_slice{0};
    if (traverse_key_view.size() > sizeof(base_node::key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(std::uint64_t));
      final_slice = false;
    } else {
      memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      final_slice = true;
    }
    std::tuple<border_node *, node_version64_body> node_and_v = find_border(key_slice);
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    std::get<tuple_node_index>(node_and_v)->lock();
    /**
     * Here, get<tuple_v_index>(node_and_v) is stable version at ending of find_border.
     * If node is not full, try split.
     */
    /**
     * Else, insert new node.
     */
forward:
    if (std::get<tuple_v_index>(node_and_v).get_deleted()) {
      goto retry;
    }
    link_or_value* lv  = std::get<tuple_node_index>(node_and_v)->get_lv_of(key_slice);


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

  /**
   *
   * @param key_slice
   * @return std::tuple<base_node*, node_version64_body>
   * node_version64_body is stable version of base_node*.
   */
  static std::tuple<border_node *, node_version64_body> find_border(base_node::key_slice_type key_slice) {
retry:
    base_node *n = get_root();
    node_version64_body v = n->get_stable_version();
    if (n != get_root()) goto retry;
descend:
    /**
     * The caller checks whether it has been deleted.
     */
    if (n->get_version_border())
      return std::make_tuple(static_cast<border_node *>(n), v);
    /**
     * @a n points to a interior_node object.
     */
    [[maybe_unused]]base_node *n_child = static_cast<interior_node *>(n)->get_child_of(key_slice);
    /**
     * As soon as you it finished operating the contents of node, read version (v_check).
     */
    node_version64_body v_check = n->get_stable_version();
    if (v == v_check) {
      /**
       * This check is different with original paper's check.
       * Original paper approach merely check whether it is locked now.
       */
      n = n_child;
      /**
       * In the original paper, this line is done outside of if scope.
       * However,  It is never used after that scope.
       * Therefore, this can execute within if scope.
       */
      v = n_child->get_stable_version();
      goto descend;
    }
    /**
     * In Original paper, v'' = stableversion(n).
     * But it is redundant that checks global current value (n.version) and loads that here.
     */

    /**
     * If the value of vsplit is different, the read location may be inappropriate.
     * Split propagates upward. It have to start from root.
     */
    if (v.get_vsplit() != v_check.get_vsplit())
      goto retry;
    /**
     * If the vsplit values ​​match, it can continue.
     * Even if it is inserted, the slot value is invariant and the order is controlled by @a permutation.
     * However, the value corresponding to the key that could not be detected may have been inserted,
     * so re-start from this node.
     */
    v = v_check;
    goto descend;
  }

  /**
   * @details
   * Todo : It will be container to be able to switch database.
   */
  static std::atomic<base_node *> root_;
};

std::atomic<base_node *> masstree_kvs::root_ = nullptr;

} // namespace yakushima
