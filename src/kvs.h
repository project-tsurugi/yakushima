/**
 * @file kvs.h
 * @brief This is the interface used by outside.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "border_helper.h"
#include "interior_node.h"
#include "scheme.h"
#include "thread_info.h"

#include "debug.hh"

namespace yakushima {

class masstree_kvs {
public:
  using key_slice_type = base_node::key_slice_type;
  using key_length_type = base_node::key_length_type;

  /**
   * @details It declares that the session starts. In a session defined as between enter and leave, it is guaranteed
   * that the heap memory object object read by get function will not be released in session. An occupied GC container
   * is assigned.
   * @param[in] token
   * @return status::OK success.
   * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
   */
  static status enter(Token &token) {
    return thread_info::assign_session(token);
  }

  /**
   * @details It declares that the session ends. Values read during the session may be invalidated from now on.
   * It will clean up the contents of GC containers that have been occupied by this session as much as possible.
   * @param[in] token
   * @return status::OK success
   * @return status::WARN_INVALID_TOKEN @a token of argument is invalid.
   */
  static status leave(Token &token) {
    return thread_info::leave_session<interior_node, border_node>(token);
  }

  /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   * @return status::OK_DESTROY_ALL destroyed all tree.
   * @return status::OK_ROOT_IS_NULL tree was nothing.
   */
  static status destroy() {
    base_node* root = base_node::get_root();
    status return_status(status::OK_ROOT_IS_NULL);
    if (root != nullptr) {
      base_node::get_root()->destroy();
      base_node::set_root(nullptr);
      return_status = status::OK_DESTROY_ALL;
    }
    return return_status;
  }

  /**
   * @brief Delete all tree from the root and release all heap objects.
   */
  static void fin() {
    destroy();
    gc_container::fin<interior_node, border_node>();
  }

  /**
   * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
   * @param[in] key_view The key_view of key-value.
   * @return std::tuple<ValueType *, std::size_t> The set of pointer to value and the value size.
   */
  template<class ValueType>
  static std::tuple<ValueType *, std::size_t>
  get(std::string_view key_view) {
retry_from_root:
    base_node *root = base_node::get_root();
    if (root == nullptr) {
      return std::make_tuple(nullptr, 0);
    }
    bool final_slice(false);
    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length;
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
      final_slice = false;
      key_slice_length = sizeof(key_slice_type);
    } else {
      memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      final_slice = true;
      key_slice_length = traverse_key_view.size();
    }
    /**
     * traverse tree to border node.
     */
    status special_status;
    std::tuple<border_node *, node_version64_body> node_and_v = find_border(root, key_slice, key_slice_length,
                                                                            special_status);
    if (special_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
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
retry_fetch_lv:
    node_version64_body v_at_fetch_lv;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv);
    /**
     * check whether it should get from this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
      /**
       * The correct border was changed between atomically fetching bordr node and atomically fetching lv.
       */
      goto retry_from_root;
    }
    /**
     * check lv_ptr == nullptr
     */
    if (lv_ptr == nullptr) {
      return std::make_tuple(nullptr, 0);
    }
    /**
     * lv_ptr != nullptr.
     */
    void *vp = lv_ptr->get_v_or_vp_();
    base_node* next_layer = lv_ptr->get_next_layer();
    /**
     * check whether vp and next_layer are fetched correctly.
     */
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete()) { // fetched vp and next_layer may be deleted.
      v_at_fb = final_check;
      goto retry_fetch_lv;
    }
    if (vp == nullptr && next_layer == nullptr) {
      /**
       * Next_layer is nullptr, meaning that value exists.
       * If it is in the case and vp is nullptr, nullptr was stored as value.
       */
      return std::make_tuple(nullptr, 0);
    }
    if (vp != nullptr) {
      if (final_slice) {
        return std::make_tuple<ValueType *, std::size_t>(reinterpret_cast<ValueType *>(vp), lv_ptr->get_value_length());
      } else {
        /**
         * value exists and not-final slice means WARN_NOT_FOUND because
         * if the key is partially the same, lv should have next_layer, but has value.
         */
        return std::make_tuple(nullptr, 0);
      }
    }
    /**
     * lv_ptr points to next_layer.
     */
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    root = next_layer;
    goto retry_find_border;
  }

  /**
   * @brief Initialize kThreadInfoTable, which is a table that holds thread execution information.
   */
  static void init() {
    /**
     * initialize thread infomation table (kThreadInfoTable)
     */
    thread_info::init();

  }

  /**
   * @pre @a token of arguments is valid.
   * @tparam ValueType If a single object is inserted, the value size and value alignment information can be
   * omitted from this type information. In this case, sizeof and alignof are executed on the type information.
   * In the cases where this is likely to cause problems and when inserting an array object,
   * the value size and value alignment information should be specified explicitly.
   * This is because sizeof for a type represents a single object size.
   * @param[in] key_view The key_view of key-value.
   * @param[in] value The pointer to value.
   * @param[in] arg_value_length The length of value object.
   * @param[in] value_align The alignment information of value object.
   * @return status::OK success.
   * @return status::WARN_UNIQUE_RESTRICTION The key-value whose key is same to given key already exists.
   */
  template<class ValueType>
  static status put(Token token, std::string_view key_view, ValueType *value,
                    std::size_t arg_value_length = sizeof(ValueType), std::size_t value_align = alignof(ValueType)) {
root_nullptr:
    base_node *expected = base_node::get_root();
    if (expected == nullptr) {
      /**
       * root is nullptr, so put single border nodes.
       */
      border_node *new_border = new border_node();
      new_border->init_border(key_view, value, true, arg_value_length, value_align);
      for (;;) {
        if (base_node::root_.compare_exchange_weak(expected, dynamic_cast<base_node *>(new_border),
                                                   std::memory_order_acq_rel, std::memory_order_acquire)) {
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
    base_node *root = base_node::get_root();
    if (root == nullptr) goto root_nullptr;

    bool final_slice{false};
    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length;
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
      final_slice = false;
      key_slice_length = sizeof(key_slice_type);
    } else {
      if (traverse_key_view.size() > 0) {
        memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      }
      final_slice = true;
      key_slice_length = traverse_key_view.size();
    }
    /**
     * traverse tree to border node.
     */
    status special_status;
    std::tuple<border_node *, node_version64_body> node_and_v = find_border(root, key_slice, key_slice_length,
                                                                            special_status);
    if (special_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
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

retry_fetch_lv:
    node_version64_body v_at_fetch_lv;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv);
    /**
     * check whether it should insert into this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
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
        lv_ptr = target_border->get_lv_of_without_lock(key_slice, key_slice_length);
        if (lv_ptr != nullptr) {
          /**
           * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
           */
          node_version64_body new_v = target_border->get_version();
          new_v.make_stable_version_forcibly(); // make this into stable_version forcibly.
          v_at_fb = v_at_fetch_lv = new_v;
          /**
           * Jump
           */
          target_border->unlock();
          goto lv_ptr_exists;
        }
      }
      std::vector<node_version64 *> lock_list;
      lock_list.emplace_back(target_border->get_version_ptr());
      insert_lv(target_border, traverse_key_view, false, value, arg_value_length, value_align, lock_list);
      for (auto &lock : lock_list) {
        lock->unlock();
      }
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
        if (target_border->get_version_deleted() // this border is incorrect.
            || target_border->get_version_vsplit() != v_at_fb.get_vsplit()) { // this border may be incorrect.
          target_border->unlock();
          goto retry_from_root;
        }
        /**
         * Here, border node is correct.
         */
        if (target_border->get_version_vdelete() != v_at_fetch_lv.get_vdelete()) { // fetched lv may be deleted.
          lv_ptr = target_border->get_lv_of_without_lock(key_slice, key_slice_length);
          if (lv_ptr == nullptr) {
            /**
             * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
             */
            node_version64_body new_v = target_border->get_version();
            new_v.make_stable_version_forcibly();
            v_at_fb = v_at_fetch_lv = new_v;
            target_border->unlock();
            goto lv_ptr_null;
          } else {
            /**
             * Before jumping, it updates the v_at_fetch by current version to improve performance lately.
             */
            node_version64_body new_v = target_border->get_version();
            new_v.make_stable_version_forcibly();
            v_at_fb = v_at_fetch_lv = new_v;
            target_border->unlock();
            goto lv_ptr_exists;
          }
        }
        /**
         * Here, lv_ptr is correct.
         * Here, not final slice, so the key_slice size is 8 bytes
         * It creates new layer and inserts this old lv into the new layer.
         */
        border_node *new_border = new border_node();
        traverse_key_view.remove_prefix(sizeof(key_slice_type));
        new_border->init_border(std::string_view{nullptr, 0}, lv_ptr->get_v_or_vp_(), true, lv_ptr->get_value_length(),
                                lv_ptr->get_value_align());
        new_border->insert_lv_at(1, traverse_key_view, true, value, arg_value_length, value_align);
        /**
         * 1st argument (index == 0) was used by this (non-final) slice at init_border func.
         * process for lv_ptr
         */
        thread_info *ti = reinterpret_cast<thread_info *>(token);
        ti->move_value_to_gc_container(lv_ptr->get_v_or_vp_());
        lv_ptr->set_need_delete_value(false);
        lv_ptr->destroy_value();
        lv_ptr->set_next_layer(new_border);
        new_border->set_parent(target_border);
        target_border->unlock();
        return status::OK;
      }
    }
    /**
     * Here, lv_ptr has some next_layer.
     */
    if (lv_ptr->get_next_layer() != nullptr) {
      root = dynamic_cast<base_node *>(lv_ptr->get_next_layer());
      /**
       * check whether border is still correct.
       */
      node_version64_body final_check = target_border->get_stable_version();
      if (final_check.get_deleted() || // this border was deleted.
          final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border may be incorrect.
        goto retry_from_root;
      }
      /**
       * check whether fetching lv is still correct.
       */
      if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete()) { // fetched lv may be deleted
        v_at_fb = final_check;
        goto retry_fetch_lv;
      }
      /**
       * root was fetched correctly.
       * root = lv; advance key; goto retry_find_border;
       */
      traverse_key_view.remove_prefix(sizeof(key_slice_type));
      goto retry_find_border;
    }
    /**
    * unreachable
    */
    std::abort();
  }

  /**
   * @pre @a token of arguments is valid.
   * @param[in] token
   * @param[in] key_view The key_view of key-value.
   * @return status::OK_ROOT_IS_NULL No existing tree.
   */
  static status remove(Token token, std::string_view key_view) {
retry_from_root:
    base_node *root = base_node::get_root();
    if (root == nullptr) {
      /**
       * root is nullptr
       */
      return status::OK_ROOT_IS_NULL;
    }

    bool final_slice{false};
    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length;
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
      final_slice = false;
      key_slice_length = sizeof(key_slice_type);
    } else {
      if (traverse_key_view.size() > 0) {
        memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      }
      final_slice = true;
      key_slice_length = traverse_key_view.size();
    }
    /**
     * traverse tree to border node.
     */
    status special_status;
    std::tuple<border_node *, node_version64_body> node_and_v = find_border(root, key_slice, key_slice_length,
                                                                            special_status);
    if (special_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
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
retry_fetch_lv:
    node_version64_body v_at_fetch_lv;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv);
    /**
     * check whether it should delete against this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
      /**
       * It may be change the correct border between atomically fetching border node and atomically fetching lv.
       */
      goto retry_from_root;
    }

    if (lv_ptr == nullptr) {
      return status::OK_NOT_FOUND;
    }
    /**
     * Here, lv_ptr != nullptr.
     * If lv_ptr has some value && final_slice
     */
    if (lv_ptr->get_v_or_vp_() != nullptr) {
      if (final_slice) {
        target_border->lock();
        node_version64_body final_check = target_border->get_version();
        if (final_check.get_deleted() || // the border was deleted.
            final_check.get_vsplit() != v_at_fb.get_vsplit()) { // the border may be incorrect.
          target_border->unlock();
          goto retry_from_root;
        }
        /**
         * here, border is correct.
         */
        if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() || // the lv may be deleted.
            final_check.get_vinsert() != v_at_fetch_lv.get_vinsert()) { // the lv may be next_layer from value ptr.
          final_check.make_stable_version_forcibly();
          v_at_fb = final_check;
          target_border->unlock();
          goto retry_fetch_lv;
        }

        target_border->delete_of(token, key_slice, key_slice_length);
        /**
         * Whether or not the lock needs to be released depends on
         * whether or not the root becomes nullptr as a result of the delete operation.
         * Let delete_of decide whether to release the lock or not.
         */
        return status::OK;
      } else {
        return status::OK_NOT_FOUND;
      }
    }
    /**
     * Here, lv_ptr has some next_layer.
     */
    if (lv_ptr->get_next_layer() != nullptr) {
      root = dynamic_cast<base_node *>(lv_ptr->get_next_layer());
      /**
       * check whether border is still correct.
       */
      node_version64_body final_check = target_border->get_stable_version();
      if (final_check.get_deleted() || // this border was deleted.
          final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border is incorrect.
        goto retry_from_root;
      }
      /**
       * check whether fetching lv is still correct.
       */
      if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete()) { // fetched lv may be deleted.
        v_at_fb = final_check; // update v_at_fb
        goto retry_fetch_lv;
      }
      /**
       * root was fetched correctly.
       * root = lv; advance key; goto retry_find_border;
       */
      traverse_key_view.remove_prefix(sizeof(key_slice_type));
      goto retry_find_border;
    }
    /**
    * unreachable
    */
    std::abort();
    return status::OK;
  }

private:
  /**
   *
   * @details It finds border node by using arguments @a root, @a key_slice.
   * If the @a root is not the root of some layer, this function finds root nodes of the layer, then finds border node by
   * using retry label.
   * @param[in] root
   * @param[in] key_slice
   * @param[in] key_slice_length
   * @param[out] special_status
   * @return std::tuple<base_node*, node_version64_body>
   * node_version64_body is stable version of base_node*.
   */
  static std::tuple<border_node *, node_version64_body>
  find_border(base_node *root, key_slice_type key_slice, key_length_type key_slice_length, status &special_status) {
retry:
    base_node *n = root;
    node_version64_body v = n->get_stable_version();
    if (v.get_deleted() || !v.get_root()) {
      special_status = status::WARN_RETRY_FROM_ROOT_OF_ALL;
      return std::make_tuple(nullptr, node_version64_body());
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
    base_node *n_child = static_cast<interior_node *>(n)->get_child_of(key_slice, key_slice_length);
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
};

} // namespace yakushima
