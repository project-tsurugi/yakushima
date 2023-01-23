/**
 * @file interface_remove.h
 */

#pragma once

#include <utility>

#include "border_node.h"
#include "kvs.h"
#include "log.h"
#include "storage.h"
#include "storage_impl.h"
#include "tree_instance.h"

namespace yakushima {

// begin - forward declaration
// end - forward declaration

[[maybe_unused]] static status remove(Token token, tree_instance* ti, // NOLINT
                                      std::string_view key_view) {
retry_from_root:
    base_node* root = ti->load_root_ptr();
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
    key_slice_type key_slice = 0;
    key_length_type key_length{};
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
        memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
        key_length = sizeof(key_slice_type) + 1;
    } else {
        if (!traverse_key_view.empty()) {
            memcpy(&key_slice, traverse_key_view.data(),
                   traverse_key_view.size());
        }
        key_length = traverse_key_view.size();
    }
    /**
      * traverse tree to border node.
      */
    status special_status{status::OK};
    std::tuple<border_node*, node_version64_body> node_and_v =
            find_border(root, key_slice, key_length, special_status);
    if (special_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        /**
          * @a root is the root node of the some layer, but it was deleted.
          * So it must retry from root of the all tree.
          */
        goto retry_from_root; // NOLINT
    }
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    border_node* target_border = std::get<tuple_node_index>(node_and_v);
#ifndef NDEBUG
    // check target_border is not nullptr
    if (target_border == nullptr) {
        LOG(ERROR) << log_location_prefix << "root: " << root
                   << ", key_slice: " << key_slice
                   << ", key_length: " << key_length
                   << ", special_status: " << special_status;
        return status::ERR_FATAL;
    }
    // check target_border is border node.
    if (typeid(*target_border) != typeid(border_node)) {
        LOG(ERROR) << log_location_prefix
                   << "find_border return not border node.";
        return status::ERR_FATAL;
    }
#endif
    node_version64_body v_at_fb = std::get<tuple_v_index>(node_and_v);
retry_fetch_lv:
    node_version64_body v_at_fetch_lv{};
    std::size_t lv_pos = 0;
    link_or_value* lv_ptr = target_border->get_lv_of(key_slice, key_length,
                                                     v_at_fetch_lv, lv_pos);
    /**
      * check whether it should delete against this node.
      */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() ||
        (v_at_fetch_lv.get_deleted() && !v_at_fetch_lv.get_root())) {
        /**
          * It may be change the correct border between atomically fetching border node 
          * and atomically fetching lv.
          */
        goto retry_from_root; // NOLINT
    }
    // the target node is correct

    if (lv_ptr == nullptr) {
        /**
         * It may be interrupted by insert or delete
         */
        node_version64_body final_check = target_border->get_stable_version();
        if (final_check.get_vinsert_delete() !=
            v_at_fetch_lv.get_vinsert_delete()) { // the lv may be inserted.
            goto retry_fetch_lv;                  // NOLINT
        }
        return status::OK_NOT_FOUND;
    }

    /**
      * Here, lv_ptr != nullptr.
      * If lv_ptr has some value && final_slice
      */
    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
        target_border->lock();
        node_version64_body final_check = target_border->get_version();
        if ((final_check.get_deleted() &&
             !final_check.get_root()) || // the border was deleted.
            final_check.get_vsplit() !=
                    v_at_fb.get_vsplit()) { // the border may be incorrect.
            target_border->version_unlock();
            goto retry_from_root; // NOLINT
        }                         // here border is correct.
        if (final_check.get_vinsert_delete() !=
            v_at_fetch_lv
                    .get_vinsert_delete()) { // the lv may be inserted/deleted.
            target_border->version_unlock();
            goto retry_fetch_lv; // NOLINT
        }

        // re-check because delete operation is not tracked.
        lv_ptr = target_border->get_lv_of_without_lock(key_slice, key_length);
        if (lv_ptr == nullptr) {
            target_border->version_unlock();
            return status::OK_NOT_FOUND;
        }

        // success delete
        target_border->delete_of<true>(token, ti, key_slice, key_length);
        return status::OK;
    }

    root = lv_ptr->get_next_layer();
    node_version64_body final_check = target_border->get_stable_version();
    if ((final_check.get_deleted() &&
         !final_check.get_root()) || // this border was deleted.
        final_check.get_vsplit() !=
                v_at_fb.get_vsplit()) { // this border is incorrect.
        goto retry_from_root;           // NOLINT
    }
    if (final_check.get_vinsert_delete() !=
        v_at_fetch_lv.get_vinsert_delete()) { // fetched lv may be deleted.
        goto retry_fetch_lv;                  // NOLINT
    }
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border; // NOLINT
}

[[maybe_unused]] static status remove(Token token, // NOLINT
                                      std::string_view storage_name,
                                      std::string_view key_view) {
    tree_instance* ti{};
    status ret{storage::find_storage(storage_name, &ti)};
    if (status::OK != ret) { return status::WARN_STORAGE_NOT_EXIST; }
    return remove(token, ti, key_view);
}

} // namespace yakushima