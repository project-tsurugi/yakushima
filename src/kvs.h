/**
 * @file kvs.h
 * @brief This is the interface used by outside.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "border_helper.h"
#include "epoch.h"
#include "interior_node.h"
#include "scan_helper.h"
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
    base_node *root = base_node::get_root();
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
    thread_info::set_epoch_thread_end();
    thread_info::join_epoch_thread();
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
    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length = traverse_key_view.size();
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
    } else {
      memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
    }
    /**
     * traverse tree to border node.
     */
    status special_status{status::OK};
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
    std::size_t lv_pos;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
    /**
     * check whether it should get from this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
      /**
       * The correct border was changed between atomically fetching bordr node and atomically fetching lv.
       */
      goto retry_from_root;
    }

    if (lv_ptr == nullptr) {
      return std::make_tuple(nullptr, 0);
    }

    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
      void *vp = lv_ptr->get_v_or_vp_();
      std::size_t v_size = lv_ptr->get_value_length();
      node_version64_body final_check = target_border->get_stable_version();
      if (final_check.get_vsplit() != v_at_fb.get_vsplit()
          || final_check.get_deleted()) {
        goto retry_from_root;
      }
      if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() ||
          final_check.get_vinsert() != v_at_fetch_lv.get_vinsert()) {
        goto retry_fetch_lv;
      }
      return std::make_tuple(reinterpret_cast<ValueType *>(vp), v_size);
    }

    base_node *next_layer = lv_ptr->get_next_layer();
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_vsplit() != v_at_fb.get_vsplit()
        || final_check.get_deleted()) {
      goto retry_from_root;
    }
    if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() ||
        final_check.get_vinsert() != v_at_fetch_lv.get_vinsert()) {
      goto retry_fetch_lv;
    }
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
    thread_info::invoke_epoch_thread();
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
  static status put(std::string_view key_view, ValueType *value,
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

    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length;
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
      key_slice_length = traverse_key_view.size();
    } else {
      if (traverse_key_view.size() > 0) {
        memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      }
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
    std::size_t lv_pos;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
    /**
     * check whether it should insert into this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
      /**
       * It may be change the correct border between atomically fetching border node and atomically fetching lv.
       */
      goto retry_from_root;
    }
    if (lv_ptr == nullptr) {
      std::vector<node_version64 *> lock_list;
      target_border->lock();
      lock_list.emplace_back(target_border->get_version_ptr());
      if (target_border->get_version_deleted() ||
          target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
        /**
         * get_version_deleted() : It was deleted between atomically fetching border node
         * and locking.
         * vsplit comparison : It may be change the correct border node between
         * atomically fetching border and lock.
         */
        node_version64::unlock(lock_list);
        goto retry_from_root;
      }
      /**
       * Here, border node is the correct.
       */
      if (target_border->get_version_vinsert() != v_at_fetch_lv.get_vinsert()) {  // It may exist lv_ptr
        /**
         * next_layers may be wrong. However, when it rechecks the next_layers, it can't get the lock down,
         * so it have to try again.
         */
        node_version64::unlock(lock_list);
        goto retry_fetch_lv;
      }
      insert_lv<interior_node, border_node>(target_border, traverse_key_view, false, value, arg_value_length,
                                            value_align, lock_list);
      node_version64::unlock(lock_list);
      return status::OK;
    }

    /**
     * Here, lv_ptr != nullptr.
     * If lv_ptr has some value && final_slice
     */
    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
      if (key_slice_length <= sizeof(key_slice_type)) {
        return status::WARN_UNIQUE_RESTRICTION;
      }
      /**
       * basically, unreachable.
       * If not a final slice, the key size is larger than the key slice type.
       * The key length of lv pointer cannot be smaller than the key slice type.
       * Therefore, it was interrupted by a parallel operation.
       */
      if (target_border->get_version_deleted()
          || target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
        goto retry_from_root;
      }
      if (target_border->get_version_vinsert() != v_at_fetch_lv.get_vinsert()
          || target_border->get_version_vdelete() != v_at_fetch_lv.get_vdelete()) {
        goto retry_fetch_lv;
      }
    }
    /**
     * Here, lv_ptr has some next_layer.
     */
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
    if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() ||
        final_check.get_vinsert() != v_at_fetch_lv.get_vinsert()) { // fetched lv may be deleted
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

    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length(traverse_key_view.size());
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
    } else {
      if (traverse_key_view.size() > 0) {
        memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
      }
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
    std::size_t lv_pos;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
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
    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
      if (key_slice_length <= sizeof(key_slice_type)) {
        target_border->lock();
        std::vector<node_version64 *> lock_list;
        lock_list.emplace_back(target_border->get_version_ptr());
        node_version64_body final_check = target_border->get_version();
        if (final_check.get_deleted() || // the border was deleted.
            final_check.get_vsplit() != v_at_fb.get_vsplit()) { // the border may be incorrect.
          node_version64::unlock(lock_list);
          goto retry_from_root;
        } // here border is correct.
        if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete()) { // the lv may be deleted.
          node_version64::unlock(lock_list);
          goto retry_fetch_lv;
        }

        target_border->delete_of<true>(token, key_slice, key_slice_length, lock_list);
        node_version64::unlock(lock_list);
        return status::OK;
      }
      return status::OK_NOT_FOUND;
    }

    root = dynamic_cast<base_node *>(lv_ptr->get_next_layer());
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_deleted() || // this border was deleted.
        final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border is incorrect.
      goto retry_from_root;
    }
    if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() || // fetched lv may be deleted.
        final_check.get_vinsert() !=
        v_at_fetch_lv.get_vinsert()) { // It may exist more closer next_layer to key of searching
      v_at_fb = final_check; // update v_at_fb
      goto retry_fetch_lv;
    }
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border;
  }

  /**
   * @details todo : add new 3 modes : try-mode : 1 trial : wait-mode : try until success : mid-mode : middle between try and wait.
   * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
   * @param[in] l_key
   * @param[in] l_exclusive
   * @param[in] r_key
   * @param[in] r_exclusive
   * @param[out] tuple_list
   * @return status::OK success.
   */
  template<class ValueType>
  static status
  scan(std::string_view l_key, bool l_exclusive, std::string_view r_key, bool r_exclusive,
       std::vector<std::tuple<ValueType *, std::size_t>> &tuple_list) {
    tuple_list.clear();
retry_from_root:
    base_node *root = base_node::get_root();
    if (root == nullptr) {
      return status::OK_ROOT_IS_NULL;
    }
    std::string_view traverse_key_view{l_key};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    key_length_type key_slice_length = traverse_key_view.size();
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
    } else {
      memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
    }
    /**
     * traverse tree to border node.
     */
    status special_status{status::OK};
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
    std::size_t lv_pos;
    link_or_value *lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);

    std::vector<std::tuple<ValueType *, std::size_t>> tuple_buffer;
    tuple_buffer.clear();
    if (target_border->get_key_length_at(lv_pos) > sizeof(key_slice_type)) {
      if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
        goto retry_from_root;
      }

      base_node *next_layer = lv_ptr->get_next_layer();
      node_version64_body final_check = target_border->get_stable_version();
      if (final_check.get_vsplit() != v_at_fb.get_vsplit()
          || final_check.get_deleted()) {
        goto retry_from_root;
      }
      if (final_check.get_vdelete() != v_at_fetch_lv.get_vdelete() ||
          final_check.get_vinsert() != v_at_fetch_lv.get_vinsert()) {
        goto retry_fetch_lv;
      }
      traverse_key_view.remove_prefix(sizeof(key_slice_type));
      root = next_layer;
      goto retry_find_border;
    }
    // here, it decides to scan from this nodes.
    for (;;) {
      status check_status = scan_border(&target_border, l_key, l_exclusive, r_key, r_exclusive, tuple_list,
                                        v_at_fetch_lv);
      if (check_status == status::OK_SCAN_END) {
        return status::OK;
      } else if (check_status == status::OK_SCAN_CONTINUE) {
        continue;
      } else if (check_status == status::OK_RETRY_FETCH_LV) {
        goto retry_fetch_lv;
      } else if (check_status == status::OK_RETRY_FROM_ROOT) {
        goto retry_from_root;
      } else {
        std::cerr << __FILE__ << ": " << __LINE__ << std::endl;
        std::abort();
      }
    }
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
