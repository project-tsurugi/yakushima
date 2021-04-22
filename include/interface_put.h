/**
 * @file interface_put.h
 */

#pragma once

#include <utility>

#include "border_node.h"
#include "interior_node.h"
#include "storage.h"
#include "storage_impl.h"

namespace yakushima {

template<class ValueType>
[[maybe_unused]] static status
put(tree_instance* ti, std::string_view key_view, ValueType* value, std::size_t arg_value_length = sizeof(ValueType),
    ValueType** created_value_ptr = nullptr, value_align_type value_align = static_cast<value_align_type>(alignof(ValueType)),
    node_version64** inserted_node_version_ptr = nullptr) {

root_nullptr:
    base_node* expected = ti->load_root_ptr();
    if (expected == nullptr) {
        /**
         * root is nullptr, so put single border nodes.
         */
        border_node* new_border = new border_node(); // NOLINT
        new_border->init_border(key_view, value, created_value_ptr, true, arg_value_length, value_align);
        for (;;) {
            if (inserted_node_version_ptr != nullptr) {
                *inserted_node_version_ptr = new_border->get_version_ptr();
            }
            base_node* desired{dynamic_cast<base_node*>(new_border)};
            if (ti->cas_root_ptr(&expected, &desired)) {
                return status::OK;
            }
            if (expected != nullptr) {
                // root is not nullptr;
                new_border->destroy();
                delete new_border; // NOLINT
                break;
            }
            // root is nullptr.
        }
    }
retry_from_root:
    /**
     * here, root is not nullptr.
     * Prepare key for traversing tree.
     */
    base_node* root = ti->load_root_ptr();
    if (root == nullptr) goto root_nullptr; // NOLINT

    std::string_view traverse_key_view{key_view};
retry_find_border:
    /**
     * prepare key_slice
     */
    key_slice_type key_slice(0);
    auto key_slice_length = static_cast<key_length_type>(traverse_key_view.size());
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
        memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
    } else {
        if (!traverse_key_view.empty()) {
            memcpy(&key_slice, traverse_key_view.data(), traverse_key_view.size());
        }
    }
    /**
     * traverse tree to border node.
     */
    status special_status{status::OK};
    std::tuple<border_node*, node_version64_body> node_and_v = find_border(root, key_slice, key_slice_length,
                                                                           special_status);
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
    node_version64_body v_at_fb = std::get<tuple_v_index>(node_and_v);

retry_fetch_lv:
    node_version64_body v_at_fetch_lv{};
    std::size_t lv_pos{0};
    link_or_value* lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
    /**
     * check whether it should insert into this node.
     */
    if ((v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit()) || v_at_fetch_lv.get_deleted()) {
        /**
         * It may be change the correct border between atomically fetching border node and atomically fetching lv.
         */
        goto retry_from_root; // NOLINT
    }
    if (lv_ptr == nullptr) {
        target_border->lock();
        if (target_border->get_version_deleted() ||
            target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
            /**
             * get_version_deleted() : It was deleted between atomically fetching border node
             * and locking.
             * vsplit comparison : It may be change the correct border node between
             * atomically fetching border and lock.
             */
            target_border->version_unlock();
            goto retry_from_root; // NOLINT
        }
        /**
         * Here, border node is the correct.
         */
        if (target_border->get_version_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // It may exist lv_ptr
            /**
             * next_layers may be wrong. However, when it rechecks the next_layers, it can't get the lock down,
             * so it have to try again.
             * TODO : It can scan (its permutation) again and insert into this border node if it don't have the same key.
             */
            target_border->version_unlock();
            goto retry_fetch_lv; // NOLINT
        }
        insert_lv<interior_node, border_node>(ti, target_border, traverse_key_view, value,
                                              reinterpret_cast<void**>(created_value_ptr), arg_value_length, // NOLINT
                                              value_align, inserted_node_version_ptr);
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
        if (target_border->get_version_deleted() || target_border->get_version_vsplit() != v_at_fb.get_vsplit()) {
            goto retry_from_root; // NOLINT
        }
        if (target_border->get_version_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {
            goto retry_fetch_lv; // NOLINT
        }
    }
    /**
     * Here, lv_ptr has some next_layer.
     */
    root = lv_ptr->get_next_layer();
    /**
     * check whether border is still correct.
     */
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_deleted() ||                        // this border was deleted.
        final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border may be incorrect.
        goto retry_from_root;                               // NOLINT
    }
    /**
     * check whether fetching lv is still correct.
     */
    if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // fetched lv may be deleted
        goto retry_fetch_lv;                                                      // NOLINT
    }
    /**
     * root was fetched correctly.
     * root = lv; advance key; goto retry_find_border;
     */
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border; // NOLINT
}

template<class ValueType>
[[maybe_unused]] static status
put(std::string_view storage_name, std::string_view key_view, ValueType* value, std::size_t arg_value_length = sizeof(ValueType),
    ValueType** created_value_ptr = nullptr, value_align_type value_align = static_cast<value_align_type>(alignof(ValueType)),
    node_version64** inserted_node_version_ptr = nullptr) {
    tree_instance* ti{};
    status ret{storage::find_storage(storage_name, &ti)};
    if (status::OK != ret) {
        return ret;
    }
    return put(ti, key_view, value, arg_value_length, created_value_ptr, value_align, inserted_node_version_ptr);
}
} // namespace yakushima