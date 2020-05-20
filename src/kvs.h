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
  get([[maybe_unused]]std::string_view key_view) {
    return std::make_tuple(nullptr, 0);
  }

  static void init_kvs() {
    root_.store(nullptr, std::memory_order_release);
  }

  template<class ValueType>
  static status put(std::string_view key_view, ValueType *value,
                    std::size_t arg_value_length = sizeof(ValueType),
                    std::size_t value_align = alignof(ValueType)) {
root_nullptr:
    base_node *expected = get_root();
    if (expected == nullptr) {
      /**
       * root is nullptr, so put single border nodes.
       */
      border_node *new_border = new border_node();
      new_border->init_border(key_view, value, true, arg_value_length, value_align);
      for (;;) {
        if (root_.compare_exchange_weak(expected, dynamic_cast<base_node *>(new_border), std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
          return status::OK;
        } else {
          if (expected != nullptr) {
            // root is not nullptr;
            new_border->destroy();
            break;
          }
          // root is nullptr.
        }
      }
    }
retry_from_root:
    /**
     * here, root is not nullptr.
     * Prepare key for traversing tree.
     */
    base_node *root = get_root();
    if (root == nullptr) goto root_nullptr;

    bool final_slice{false};
    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    base_node::key_slice_type key_slice;
    if (traverse_key_view.size() > sizeof(base_node::key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(base_node::key_slice_type));
      final_slice = false;
    } else {
      memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      final_slice = true;
    }
    /**
     * traverse tree to border node.
     */
    status special_status;
    std::tuple<border_node *, node_version64_body> node_and_v = find_border(root, key_slice, special_status);
    if (special_status == status::WARN_ROOT_DELETED) {
      /**
       * @a root is the root node of the some layer, but it was deleted.
       * So it must retry from root of the all tree.
       */
      goto retry_from_root;
    }
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    border_node *target_border = std::get<tuple_node_index>(node_and_v);
    node_version64_body v_at_fb = std::get<tuple_v_index>(node_and_v);
    /**
     * check whether it should insert into this node.
     */
    node_version64_body v_at_fetch_lv;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, v_at_fetch_lv);
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit()) {
      /**
       * It may be change the correct border between atomically fetching border node and atomically fetching lv.
       */
      goto retry_from_root;
    }
    /**
     * if lv == nullptr : insert node into this border_node.
     * else if lv == some value && final_slice : return status::WARN_UNIQUE_RESTRICTION.
     * else if lv ==  next_layer && final_slice == true : return status::WARN_UNIQUE_RESTRICTION.
     * else if lv == next_layer && final_slice == false : root = lv; advance key; goto retry_find_border;
     * else unreachable; std::abort();
     */
lv_ptr_null:
    if (lv_ptr == nullptr) {
      /**
       * inserts node into this border_node.
       */
      target_border->lock();
      if (target_border->get_version_deleted()
          || target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
        /**
         * get_version_deleted() : It was deleted between atomically fetching border node
         * and locking.
         * vsplit comparison : It may be change the correct border node between
         * atomically fetching border and lock.
         */
        target_border->unlock();
        goto retry_from_root;
      }
      /**
       * Here, border node is the correct.
       */
      if (target_border->get_version_vinsert() != v_at_fetch_lv.get_vinsert()
          || target_border->get_version_vdelete() != v_at_fetch_lv.get_vdelete()) {
        /**
         * It must re-check the existence of lv because it was inserted or deleted between
         * fetching lv and locking.
         */
        lv_ptr = target_border->get_lv_of_without_lock(key_slice);
        if (lv_ptr != nullptr) {
          /**
           * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
           */
          node_version64_body new_v = target_border->get_version();
          new_v.set_locked(false);
          v_at_fb = v_at_fetch_lv = new_v;
          /**
           * Jump
           */
          target_border->unlock();
          goto lv_ptr_exists;
        }
      }
      target_border->insert_lv(traverse_key_view, value, arg_value_length, value_align);
      target_border->unlock();
      return status::OK;
    }
lv_ptr_exists:
    /**
     * Here, lv_ptr != nullptr.
     * If lv_ptr has some value && final_slice
     */
    if (lv_ptr->get_v_or_vp_() != nullptr) {
      if (final_slice) {
        return status::WARN_UNIQUE_RESTRICTION;
      } else {
        /**
         * Finally, It creates new layer and inserts this old lv into the new layer.
         */
        target_border->lock();
        if (target_border->get_version_deleted()
            || target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
          target_border->unlock();
          goto retry_from_root;
        }
        /**
         * Here, border node is correct.
         */
        if (target_border->get_version_vinsert() != v_at_fetch_lv.get_vinsert()
            || target_border->get_version_vdelete() != v_at_fetch_lv.get_vdelete()) {
          /**
           * lv_ptr may be incorrect.
           */
          lv_ptr = target_border->get_lv_of_without_lock(key_slice);
          if (lv_ptr == nullptr) {
            /**
             * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
             */
            node_version64_body new_v = target_border->get_version();
            new_v.set_locked(false);
            v_at_fb = v_at_fetch_lv = new_v;
            target_border->unlock();
            goto lv_ptr_null;
          } else {
            /**
             * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
             */
            node_version64_body new_v = target_border->get_version();
            new_v.set_locked(false);
            v_at_fb = v_at_fetch_lv = new_v;
            target_border->unlock();
            goto lv_ptr_exists;
          }
        }
        /**
         * Here, lv_ptr is correct.
         * It creates new layer and inserts this old lv into the new layer.
         */
        border_node *new_border = new border_node();
        new_border->init_border(traverse_key_view, value, true, arg_value_length, value_align);
        /**
         * Here, not final slice, so the key_slice size is 8 bytes
         */
        std::string_view slice_of_traverse_key_view(traverse_key_view);
        slice_of_traverse_key_view.remove_suffix(slice_of_traverse_key_view.size() - sizeof(base_node::key_slice_type));
        /**
         * 1st argument (index == 0) was used by this (non-final) slice at init_border func.
         */
        new_border->insert_lv_at(1, slice_of_traverse_key_view, lv_ptr->get_v_or_vp_(), lv_ptr->get_value_length(),
                                 lv_ptr->get_value_align());
        /**
         * process for lv_ptr
         */
        lv_ptr->destroy_value();
        lv_ptr->set_next_layer(dynamic_cast<base_node *>(new_border));
        target_border->unlock();
        return status::OK;
      }
    }
    /**
     * Here, lv_ptr has some next_layer.
     */
    if (lv_ptr->get_next_layer() != nullptr && !final_slice) {
      if (target_border->get_version_deleted() ||
          target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
        /**
         * border is incorrect.
         */
        target_border->unlock();
        goto retry_from_root;
      }
      /**
       * root = lv; advance key; goto retry_find_border;
       */
      root = dynamic_cast<base_node *>(lv_ptr->get_next_layer());
      traverse_key_view.remove_prefix(sizeof(base_node::key_slice_type));
      target_border->unlock();
      goto retry_find_border;
    }
    /**
    * unreachable
    */
    std::abort();
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
   * @details It finds border node by using arguments @a root, @a key_slice.
   * If the @a root is not the root of some layer, this function finds root nodes of the layer, then finds border node by
   * using retry label.
   * @return std::tuple<base_node*, node_version64_body>
   * node_version64_body is stable version of base_node*.
   */
  static std::tuple<border_node *, node_version64_body>
  find_border(base_node *root, base_node::key_slice_type key_slice, status &special_status) {
retry:
    base_node *n = root;
    node_version64_body v = n->get_stable_version();
    if (v.get_root() && v.get_deleted()) {
      special_status = status::WARN_ROOT_DELETED;
      return std::make_tuple(nullptr, node_version64_body());
    }
    /**
     * Here, valid node.
     */
    if (n != get_root()) {
      root = root->get_parent();
      goto retry;
    }
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
