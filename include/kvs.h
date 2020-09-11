/**
 * @file kvs.h
 * @brief This is the interface used by outside.
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "border_helper.h"
#include "common_helper.h"
#include "epoch.h"
#include "interior_node.h"
#include "manager_thread.h"
#include "scan_helper.h"
#include "scheme.h"
#include "gc_info_table.h"

namespace yakushima {

using key_slice_type = base_node::key_slice_type;
using key_length_type = base_node::key_length_type;

/**
 * @details It declares that the session starts. In a session defined as between enter and leave, it is guaranteed
 * that the heap memory object object read by get function will not be released in session. An occupied GC container
 * is assigned.
 * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
 * @return status::OK success.
 * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
 */
[[maybe_unused]] static status enter(Token &token) {
    return gc_info_table::assign_gc_info(token);
}

/**
 * @details It declares that the session ends. Values read during the session may be invalidated from now on.
 * It will clean up the contents of GC containers that have been occupied by this session as much as possible.
 * @param[in] token
 * @return status::OK success
 * @return status::WARN_INVALID_TOKEN @a token of argument is invalid.
 */
[[maybe_unused]] static status leave(Token token) {
    return gc_info_table::leave_gc_info<interior_node, border_node>(token);
}

/**
 * @brief release all heap objects and clean up.
 * @pre This function is called by single thread.
 * @return status::OK_DESTROY_ALL destroyed all tree.
 * @return status::OK_ROOT_IS_NULL tree was nothing.
 */
[[maybe_unused]] static status destroy() {
    base_node* root{base_node::get_root_ptr()};
    if (root == nullptr) return status::OK_ROOT_IS_NULL;
    base_node::get_root_ptr()->destroy();
    delete base_node::get_root_ptr(); // NOLINT
    base_node::set_root(nullptr);
    return status::OK_DESTROY_ALL;
}

/**
 * @brief Delete all tree from the root, release all heap objects, and join epoch thread.
 */
[[maybe_unused]] static void fin() {
    destroy();
    epoch_manager::set_epoch_thread_end();
    epoch_manager::join_epoch_thread();
    gc_container::fin<interior_node, border_node>();
}

/**
 * @brief Get value which is corresponding to given @a key_view.
 * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
 * @param[in] key_view The key_view of key-value.
 * @return std::pair<ValueType *, std::size_t> The pair of pointer to value and the value size.
 */
template<class ValueType>
[[maybe_unused]] static std::pair<ValueType*, std::size_t> get(std::string_view key_view) {
retry_from_root:
    base_node* root = base_node::get_root_ptr();
    if (root == nullptr) {
        return std::make_pair(nullptr, 0);
    }
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
     * check whether it should get from this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
        /**
         * The correct border was changed between atomically fetching border node and atomically fetching lv.
         */
        goto retry_from_root; // NOLINT
    }

    if (lv_ptr == nullptr) {
        return std::make_pair(nullptr, 0);
    }

    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
        void* vp = lv_ptr->get_v_or_vp_();
        std::size_t v_size = lv_ptr->get_value_length();
        node_version64_body final_check = target_border->get_stable_version();
        if (final_check.get_vsplit() != v_at_fb.get_vsplit()
            || final_check.get_deleted()) {
            goto retry_from_root; // NOLINT
        }
        if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {
            goto retry_fetch_lv; // NOLINT
        }
        return std::make_pair(reinterpret_cast<ValueType*>(vp), v_size); // NOLINT
    }

    root = lv_ptr->get_next_layer();
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_vsplit() != v_at_fb.get_vsplit()
        || final_check.get_deleted()) {
        goto retry_from_root; // NOLINT
    }
    if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {
        goto retry_fetch_lv; // NOLINT
    }
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border; // NOLINT
}

/**
 * @brief Initialize kThreadInfoTable which is a table that holds thread execution information about garbage
 * collection and invoke epoch thread.
 */
[[maybe_unused]] static void init() {
    /**
     * initialize thread information table (kThreadInfoTable)
     */
    gc_info_table::init();
    epoch_manager::invoke_epoch_thread();
}

/**
 * @biref Put the value with given @a key_view.
 * @pre @a token of arguments is valid.
 * @tparam ValueType If a single object is inserted, the value size and value alignment information can be
 * omitted from this type information. In this case, sizeof and alignof are executed on the type information.
 * In the cases where this is likely to cause problems and when inserting_deleting an array object,
 * the value size and value alignment information should be specified explicitly.
 * This is because sizeof for a type represents a single object size.
 * @param[in] key_view The key_view of key-value.
 * @param[in] value The pointer to given value.
 * @param[out] created_value_ptr The pointer to created value in yakushima.
 * @param[in] arg_value_length The length of value object.
 * @param[in] value_align The alignment information of value object.
 * @return status::OK success.
 * @return status::WARN_UNIQUE_RESTRICTION The key-value whose key is same to given key already exists.
 */
template<class ValueType>
[[maybe_unused]] static status
put(std::string_view key_view, ValueType* value, std::size_t arg_value_length = sizeof(ValueType),
    ValueType** created_value_ptr = nullptr,
    value_align_type value_align = static_cast<value_align_type>(alignof(ValueType))) {
root_nullptr:
    base_node* expected = base_node::get_root_ptr();
    if (expected == nullptr) {
        /**
         * root is nullptr, so put single border nodes.
         */
        border_node* new_border = new border_node(); // NOLINT
        new_border->init_border(key_view, value, created_value_ptr, true, arg_value_length, value_align);
        for (;;) {
            if (base_node::get_root().compare_exchange_weak(expected, new_border,
                                                            std::memory_order_acq_rel, std::memory_order_acquire)) {
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
    base_node* root = base_node::get_root_ptr();
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
        if (target_border->get_version_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {  // It may exist lv_ptr
            /**
             * next_layers may be wrong. However, when it rechecks the next_layers, it can't get the lock down,
             * so it have to try again.
             */
            target_border->version_unlock();
            goto retry_fetch_lv; // NOLINT
        }
        insert_lv<interior_node, border_node>(target_border, traverse_key_view, value,
                                              reinterpret_cast<void**>(created_value_ptr), arg_value_length,  // NOLINT
                                              value_align);
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
    if (final_check.get_deleted() || // this border was deleted.
        final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border may be incorrect.
        goto retry_from_root; // NOLINT
    }
    /**
     * check whether fetching lv is still correct.
     */
    if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) {// fetched lv may be deleted
        goto retry_fetch_lv; // NOLINT
    }
    /**
     * root was fetched correctly.
     * root = lv; advance key; goto retry_find_border;
     */
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border; // NOLINT
}

/**
 * @pre @a token of arguments is valid.
 * @param[in] token
 * @param[in] key_view The key_view of key-value.
 * @return status::OK_ROOT_IS_NULL No existing tree.
 */
[[maybe_unused]] static status remove(Token token, std::string_view key_view) {
retry_from_root:
    base_node* root = base_node::get_root_ptr();
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
    std::size_t lv_pos = 0;
    link_or_value* lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
    /**
     * check whether it should delete against this node.
     */
    if (v_at_fetch_lv.get_vsplit() != v_at_fb.get_vsplit() || v_at_fetch_lv.get_deleted()) {
        /**
         * It may be change the correct border between atomically fetching border node and atomically fetching lv.
         */
        goto retry_from_root; // NOLINT
    }

    if (lv_ptr == nullptr) {
        node_version64_body final_check = target_border->get_version();
        if (final_check.get_deleted() || // the border was deleted.
            final_check.get_vsplit() != v_at_fb.get_vsplit()) { // the border may be incorrect.
            goto retry_from_root; // NOLINT
        } // here border is correct.
        if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // the lv may be inserted/deleted.
            goto retry_fetch_lv; // NOLINT
        }

        return status::OK_NOT_FOUND;
    }
    /**
     * Here, lv_ptr != nullptr.
     * If lv_ptr has some value && final_slice
     */
    if (target_border->get_key_length_at(lv_pos) <= sizeof(key_slice_type)) {
        target_border->lock();
        std::vector<node_version64*> lock_list;
        lock_list.emplace_back(target_border->get_version_ptr());
        node_version64_body final_check = target_border->get_version();
        if (final_check.get_deleted() || // the border was deleted.
            final_check.get_vsplit() != v_at_fb.get_vsplit()) { // the border may be incorrect.
            node_version64::unlock(lock_list);
            goto retry_from_root; // NOLINT
        } // here border is correct.
        if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // the lv may be inserted/deleted.
            node_version64::unlock(lock_list);
            goto retry_fetch_lv; // NOLINT
        }

        target_border->delete_of<true>(token, key_slice, key_slice_length, lock_list);
        node_version64::unlock(lock_list);
        return status::OK;
    }

    root = lv_ptr->get_next_layer();
    node_version64_body final_check = target_border->get_stable_version();
    if (final_check.get_deleted() || // this border was deleted.
        final_check.get_vsplit() != v_at_fb.get_vsplit()) { // this border is incorrect.
        goto retry_from_root; // NOLINT
    }
    if (final_check.get_vinsert_delete() != v_at_fetch_lv.get_vinsert_delete()) { // fetched lv may be deleted.
        goto retry_fetch_lv; // NOLINT
    }
    traverse_key_view.remove_prefix(sizeof(key_slice_type));
    goto retry_find_border; // NOLINT
}

/**
 * TODO : add new 3 modes : try-mode : 1 trial : wait-mode : try until success : mid-mode : middle between try and wait.
 */
/**
 * @brief scan range between @a l_key and @a r_key.
 * @tparam ValueType The returned pointer is cast to the given type information before it is returned.
 * @param[in] l_key An argument that specifies the left endpoint.
 * @param[in] l_end If this argument is scan_endpoint :: EXCLUSIVE, the interval does not include the endpoint.
 * If this argument is scan_endpoint :: INCLUSIVE, the interval contains the endpoint.
 * If this is scan_endpoint :: INF, there is no limit on the interval in left direction. And ignore @a l_key.
 * @param[in] r_key An argument that specifies the right endpoint.
 * @param[in] r_end If this argument is scan_endpoint :: EXCLUSIVE, the interval does not include the endpoint.
 * If this argument is scan_endpoint :: INCLUSIVE, the interval contains the endpoint.
 * If this is scan_endpoint :: INF, there is no limit on the interval in right direction. And ignore @a r_key.
 * @param[out] tuple_list A set with a pointer to value and size as a result of this function.
 * @param[out] node_version_vec The set of node_version for transaction processing (protection of phantom problems).
 * If you don't use yakushima for transaction processing, you don't have to use this argument.
 * @return status::OK success.
 */
template<class ValueType>
[[maybe_unused]] static status
scan(std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
     std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>* node_version_vec = nullptr) {
    /**
     * Prohibition : std::string_view{nullptr, non-zero value}.
     */
    if ((l_key.data() == nullptr && !l_key.empty()) ||
        (r_key.data() == nullptr && !r_key.empty())) {
        return status::ERR_BAD_USAGE;
    }

    /**
     * Case of l_key == r_key.
     * 1 : l_end == r_end == scan_endpoint::INCLUSIVE, only one point that matches the endpoint can be scanned.
     * 2 : l_end == r_end == scan_endpoint::INF, all range.
     */
    if (l_key == r_key && !((l_end == scan_endpoint::INCLUSIVE && r_end == scan_endpoint::INCLUSIVE) ||
                            (l_end == scan_endpoint::INF && r_end == scan_endpoint::INF))) {
        return status::ERR_BAD_USAGE;
    }

    tuple_list.clear();
    if (node_version_vec != nullptr) {
        node_version_vec->clear();
    }
retry_from_root:
    base_node* root = base_node::get_root_ptr();
    if (root == nullptr) {
        return status::OK_ROOT_IS_NULL;
    }
    std::string_view traverse_key_view{l_key};
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
    status check_status{status::OK};
    std::tuple<border_node*, node_version64_body> node_and_v = find_border(root, key_slice, key_slice_length,
                                                                           check_status);
    if (check_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        /**
         * @a root is the root node of the some layer, but it was deleted.
         * So it must retry from root of the all tree.
         */
        goto retry_from_root; // NOLINT
    }
    constexpr std::size_t tuple_node_index = 0;
    border_node* target_border = std::get<tuple_node_index>(node_and_v);
retry_fetch_lv:
    node_version64_body v_at_fetch_lv{};
    std::size_t lv_pos{0};
    link_or_value* lv_ptr = target_border->get_lv_of(key_slice, key_slice_length, v_at_fetch_lv, lv_pos);
    [[maybe_unused]] std::size_t kl{0}; // NOLINT
    [[maybe_unused]] base_node* next_layer{nullptr}; // NOLINT
    if (lv_ptr != nullptr) {
        kl = target_border->get_key_length_at(lv_pos);
        next_layer = lv_ptr->get_next_layer();
    }

    check_status = scan_check_retry<ValueType>(target_border, v_at_fetch_lv);
    if (check_status == status::OK_RETRY_FROM_ROOT) goto retry_from_root; // NOLINT
    else if (check_status == status::OK_RETRY_FETCH_LV) goto retry_fetch_lv; // NOLINT

    if (lv_ptr != nullptr &&
        kl > sizeof(key_slice_type)) {
        traverse_key_view.remove_prefix(sizeof(key_slice_type));
        root = next_layer;
        goto retry_find_border; // NOLINT
    }
    // here, it decides to scan from this nodes.
    for (;;) {
        check_status = scan_border<ValueType>(&target_border, traverse_key_view, l_end, r_key, r_end,
                                              tuple_list, v_at_fetch_lv, node_version_vec);
        if (check_status == status::OK_SCAN_END) {
            return status::OK;
        }
        if (check_status == status::OK_SCAN_CONTINUE) {
            continue;
        }
        if (check_status == status::OK_RETRY_FETCH_LV) {
            goto retry_fetch_lv; // NOLINT
        } else if (check_status == status::OK_RETRY_FROM_ROOT) {
            goto retry_from_root; // NOLINT
        } else {
            // unreachable
            std::cerr << __FILE__ << ": " << __LINE__ << std::endl;
            std::abort();
        }
    }
}

} // namespace yakushima
