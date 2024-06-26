/**
 * @file scan_helper.h
 */

#pragma once

#include "base_node.h"
#include "border_node.h"
#include "common_helper.h"
#include "interior_node.h"
#include "scheme.h"

namespace yakushima {

// forward declaration
template<class ValueType>
static status
scan_border(border_node** target, std::string_view l_key, scan_endpoint l_end,
            std::string_view r_key, scan_endpoint r_end,
            std::vector<std::tuple<std::string, ValueType*, std::size_t>>&
                    tuple_list,
            node_version64_body& v_at_fb,
            std::vector<std::tuple<std::string, node_version64_body,
                                   node_version64*>>* node_version_vec,
            const std::string& key_prefix, std::size_t max_size);

inline status scan_check_retry(border_node* const bn,
                               node_version64_body& v_at_fb) {
    node_version64_body check = bn->get_stable_version();
    if (check != v_at_fb) {
        // fail optimistic verify
        if (check.get_vsplit() != v_at_fb.get_vsplit() || check.get_deleted()) {
            /**
             * The node at find border was changed by split or deleted.
             */
            return status::OK_RETRY_FROM_ROOT;
        }
        /**
         * The structure of the border node was not changed.
         * So reading border node can retry from that.
         */
        v_at_fb = check;
        return status::OK_RETRY_AFTER_FB;
    }
    return status::OK;
}

/**
 * scan for some try nodes which is not root.
 */
template<class ValueType>
static status
scan(base_node* const root, const std::string_view l_key,
     const scan_endpoint l_end, const std::string_view r_key,
     const scan_endpoint r_end,
     std::vector<std::tuple<std::string, ValueType*, std::size_t>>& tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>* const
             node_version_vec,
     const std::string& key_prefix, const std::size_t max_size) {
    /**
     * Log size before scanning this node.
     * This must be before retry label for retry at find border.
     */
    std::size_t initial_size_of_tuple_list{tuple_list.size()};
    std::size_t initial_size_of_node_version_vec{};
    if (node_version_vec != nullptr) {
        initial_size_of_node_version_vec = node_version_vec->size();
    }

    /**
     * For retry of failing optimistic verify, it must erase parts of
     * tuple_list and node vec. clear between initial_size... and current size.
     * about tuple_list.
     */
    auto clean_up_tuple_list_nvc = [&tuple_list,
                                    &node_version_vec](std::size_t isoftl,
                                                       std::size_t isonvv) {
        if (tuple_list.size() != isoftl) {
            std::size_t erase_num = tuple_list.size() - isoftl;
            tuple_list.erase(tuple_list.end() - erase_num, tuple_list.end());
        }
        // about node_version_vec
        if (node_version_vec != nullptr) {
            if (node_version_vec->size() != isonvv) {
                std::size_t erase_num = node_version_vec->size() - isonvv;
                node_version_vec->erase(node_version_vec->end() - // NOLINT
                                                erase_num,        // NOLINT
                                        node_version_vec->end());
            }
        }
    };

retry:
    if (root->get_version_deleted() || !root->get_version_root()) {
        return status::OK_RETRY_FROM_ROOT;
    }

    std::tuple<border_node*, node_version64_body> node_and_v;
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    status check_status{};
    key_slice_type ks{0};
    key_length_type kl = l_key.size(); // NOLINT
    if (l_key.size() > sizeof(key_slice_type)) {
        memcpy(&ks, l_key.data(), sizeof(key_slice_type));
    } else {
        if (!l_key.empty()) { memcpy(&ks, l_key.data(), l_key.size()); }
    }
    node_and_v = find_border(root, ks, kl, check_status);
    if (check_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        return status::OK_RETRY_AFTER_FB;
    }
    border_node* bn(std::get<tuple_node_index>(node_and_v));
    node_version64_body check_v = std::get<tuple_v_index>(node_and_v);

    for (;;) {
        // log size before scan_border
        std::size_t initial_size_of_tuple_list_at_fb{tuple_list.size()};
        std::size_t initial_size_of_node_version_vec_at_fb{};
        if (node_version_vec != nullptr) {
            initial_size_of_node_version_vec_at_fb = node_version_vec->size();
        }

        // scan the border node
        check_status = scan_border<ValueType>(
                &bn, l_key, l_end, r_key, r_end, tuple_list, check_v,
                node_version_vec, key_prefix, max_size);

        // check rc, success
        if (check_status == status::OK_SCAN_END) { return status::OK; }
        if (check_status == status::OK_SCAN_CONTINUE) { continue; }

        /**
         * fail. it doesn't need to clear tuple and node information because
         * caller of this will do.
         */
        if (check_status == status::OK_RETRY_AFTER_FB) {
            node_version64_body re_check_v = bn->get_stable_version();
            if (check_v.get_vsplit() != re_check_v.get_vsplit() ||
                // retry from this b+ tree
                re_check_v.get_deleted()) {
                return status::OK_RETRY_AFTER_FB;
            }
            if (check_v.get_vinsert_delete() !=
                re_check_v.get_vinsert_delete()) {
                // retry from this border node
                check_v = re_check_v;
                clean_up_tuple_list_nvc(initial_size_of_tuple_list_at_fb,
                                        initial_size_of_node_version_vec_at_fb);
                continue;
            }
        } else if (check_status == status::OK_RETRY_FROM_ROOT) {
            clean_up_tuple_list_nvc(initial_size_of_tuple_list,
                                    initial_size_of_node_version_vec);
            goto retry; // NOLINT
        }
    }
}

/**
 * scan for some leafnode of b+tree.
 */
template<class ValueType>
static status
scan_border(border_node** const target, const std::string_view l_key,
            const scan_endpoint l_end, const std::string_view r_key,
            const scan_endpoint r_end,
            std::vector<std::tuple<std::string, ValueType*, std::size_t>>&
                    tuple_list,
            node_version64_body& v_at_fb,
            std::vector<std::pair<node_version64_body, node_version64*>>* const
                    node_version_vec,
            const std::string& key_prefix, const std::size_t max_size) {
    /**
     * Log size before scanning this node.
     * This must be before retry label for retry at find border.
     */
    std::size_t initial_size_of_tuple_list{tuple_list.size()};
    std::size_t initial_size_of_node_version_vec{};
    if (node_version_vec != nullptr) {
        initial_size_of_node_version_vec = node_version_vec->size();
    }
    /**
     * For retry of failing optimistic verify, it must erase parts of
     * tuple_list and node vec. clear between initial_size... and current size.
     * about tuple_list.
     */
    auto clean_up_tuple_list_nvc = [&tuple_list, &node_version_vec,
                                    initial_size_of_tuple_list,
                                    initial_size_of_node_version_vec]() {
        if (tuple_list.size() != initial_size_of_tuple_list) {
            std::size_t erase_num =
                    tuple_list.size() - initial_size_of_tuple_list;
            tuple_list.erase(tuple_list.end() - erase_num, tuple_list.end());
        }
        // about node_version_vec
        if (node_version_vec != nullptr) {
            if (node_version_vec->size() != initial_size_of_node_version_vec) {
                std::size_t erase_num = node_version_vec->size() -
                                        initial_size_of_node_version_vec;
                node_version_vec->erase(node_version_vec->end() - // NOLINT
                                                erase_num,        // NOLINT
                                        node_version_vec->end());
            }
        }
    };
retry:

    /**
     * This is used below loop for logging whether this scan catches some
     * elements in this node.
     */
    bool tuple_pushed_num{false};

    border_node* bn = *target;
    /**
     * next node pointer must be logged before optimistic verify.
     */
    border_node* next = bn->get_next();
    /**
     * get permutation at once.
     * After scan border, optimistic verify support this is atomic.
     */
    permutation perm(bn->get_permutation().get_body());
    // check all elements in border node.
    for (std::size_t i = 0; i < perm.get_cnk(); ++i) {
        std::size_t index = perm.get_index_of_rank(i);
        key_slice_type ks = bn->get_key_slice_at(index);
        key_length_type kl = bn->get_key_length_at(index);
        std::string full_key{key_prefix};
        if (kl > 0) {
            // gen full key from log and this key slice
            full_key.append(
                    reinterpret_cast<char*>(&ks), // NOLINT
                    kl < sizeof(key_slice_type) ? kl : sizeof(key_slice_type));
            /**
             * If the key is complete (kl < sizeof(key_slice_type)), the key
             * slice must be copied by the size of key length.
             * Otherwise, sizeof key_slice_type.
             */
        }
        link_or_value* lv = bn->get_lv_at(index);
        value* vp = lv->get_value();
        base_node* next_layer = lv->get_next_layer();
        node_version64* node_version_ptr = bn->get_version_ptr();
        /**
         * This verification may seem verbose, but it can also be considered
         * an early abort.
         */
        status check_status = scan_check_retry(bn, v_at_fb);
        if (check_status != status::OK) {
            // failed. clean up tuple list and node vesion vec.
            clean_up_tuple_list_nvc();
        }
        if (check_status == status::OK_RETRY_FROM_ROOT) {
            return status::OK_RETRY_FROM_ROOT;
        }
        if (check_status == status::OK_RETRY_AFTER_FB) {
            goto retry; // NOLINT
        }
        if (kl > sizeof(key_slice_type)) {
            std::string_view arg_l_key;
            scan_endpoint arg_l_end{};
            if (l_end == scan_endpoint::INF) {
                arg_l_key = "";
                arg_l_end = scan_endpoint::INF;
            } else {
                key_slice_type l_key_slice{0};
                memcpy(&l_key_slice, l_key.data(),
                       l_key.size() < sizeof(key_slice_type)
                               ? l_key.size()
                               : sizeof(key_slice_type));
                // check left point
                int ret_cmp = memcmp(&l_key_slice, &ks, sizeof(key_slice_type));
                if (ret_cmp < 0) {
                    arg_l_key = "";
                    arg_l_end = scan_endpoint::INF;
                } else if (ret_cmp == 0) {
                    arg_l_key = l_key;
                    if (arg_l_key.size() > sizeof(key_slice_type)) {
                        arg_l_key.remove_prefix(sizeof(key_slice_type));
                    } else {
                        arg_l_key = "";
                    }
                    arg_l_end = l_end;
                } else {
                    continue;
                    /**
                     * Ignore it because it is smaller than the left end point.
                     */
                }
            }
            std::string_view arg_r_key;
            scan_endpoint arg_r_end{};
            if (r_end == scan_endpoint::INF) {
                arg_r_key = "";
                arg_r_end = scan_endpoint::INF;
            } else {
                int ret_cmp = memcmp(r_key.data(), full_key.data(),
                                     r_key.size() < full_key.size()
                                             ? r_key.size()
                                             : full_key.size());
                if (ret_cmp < 0) { return status::OK_SCAN_END; }
                if (ret_cmp == 0) {
                    if (r_key.size() <= full_key.size()) {
                        return status::OK_SCAN_END;
                    }
                    arg_r_key = r_key;
                    arg_r_end = r_end;
                } else {
                    arg_r_key = "";
                    arg_r_end = scan_endpoint::INF;
                }
            }
            check_status =
                    scan(next_layer, arg_l_key, arg_l_end, arg_r_key, arg_r_end,
                         tuple_list, node_version_vec, full_key, max_size);
            if (check_status != status::OK) {
                // failed. clean up tuple list and node vesion vec.
                clean_up_tuple_list_nvc();
                goto retry; // NOLINT
            }
        } else {
            auto in_range = [&full_key, &tuple_list, &vp, &node_version_vec,
                             &v_at_fb, &node_version_ptr, &tuple_pushed_num,
                             max_size]() {
                tuple_list.emplace_back(std::make_tuple(
                        full_key, static_cast<ValueType*>(value::get_body(vp)),
                        value::get_len(vp)));
                if (node_version_vec != nullptr) {
                    /**
                     * note:
                     * std::get<1>(node_version_vec.back()) != node_version_ptr
                     * Adding this can reduce redundant emplace_back. However,
                     * the correspondence between the value of the scan result
                     * and the pointer to the node version becomes unknown,
                     * making it impossible to perform node verify according
                     * to the actual situation read by the transaction
                     * execution engine.
                     */
                    node_version_vec->emplace_back(
                            std::make_pair(v_at_fb, node_version_ptr));
                }
                tuple_pushed_num = true;
                if (max_size != 0 && tuple_list.size() >= max_size) {
                    return status::OK_SCAN_END;
                }
                return status::OK;
            };
            if (l_end == scan_endpoint::INF && r_end == scan_endpoint::INF) {
                // all range
                if (in_range() != status::OK) return status::OK_SCAN_END;
                continue;
            }
            // not all range
            if (l_end != scan_endpoint::INF) {
                key_slice_type l_key_slice{0};
                if (!l_key.empty()) {
                    memcpy(&l_key_slice, l_key.data(),
                           l_key.size() < sizeof(key_slice_type)
                                   ? l_key.size()
                                   : sizeof(key_slice_type));
                }
                int l_cmp = memcmp(&l_key_slice, &ks, sizeof(key_slice_type));
                if (l_cmp > 0 ||
                    (l_cmp == 0 && (l_key.size() > kl ||
                                    (l_key.size() == kl &&
                                     l_end == scan_endpoint::EXCLUSIVE)))) {
                    continue;
                }
            }
            // pass left endpoint.
            if (r_end == scan_endpoint::INF) {
                if (in_range() != status::OK) return status::OK_SCAN_END;
                continue;
            }
            int r_cmp =
                    memcmp(r_key.data(), full_key.data(),
                           r_key.size() < full_key.size() ? r_key.size()
                                                          : full_key.size());
            if (r_cmp > 0 ||
                (r_cmp == 0 && (r_key.size() > full_key.size() ||
                                (r_key.size() == full_key.size() &&
                                 r_end == scan_endpoint::INCLUSIVE)))) {
                if (in_range() != status::OK) { return status::OK_SCAN_END; }
                continue;
            }
            // pass right endpoint.
            if (!tuple_pushed_num && node_version_vec != nullptr) {
                /**
                 * Since it is a rightmost node included in the range, it is
                 * included in the phantom verification. However, there were
                 * no elements included in the range.
                 */
                node_version_vec->emplace_back(
                        std::make_pair(v_at_fb, bn->get_version_ptr()));
            }

            return status::OK_SCAN_END;
        }
    }
    // done about checking for all elements of border node.

    if (!tuple_pushed_num && node_version_vec != nullptr) {
        /**
         * Since it is a leftmost node included in the range, it is included
         * in the phantom verification. However, there were no elements
         * included in the range.
         */
        node_version_vec->emplace_back(
                std::make_pair(v_at_fb, bn->get_version_ptr()));
    }

    // log before verify for atomicity
    node_version64_body next_version{};
    if (next != nullptr) { next_version = next->get_stable_version(); }

    // final check for atomicity
    status check_status = scan_check_retry(bn, v_at_fb);
    if (check_status != status::OK) {
        // failed. clean up tuple list and node vesion vec.
        clean_up_tuple_list_nvc();
    }
    if (check_status == status::OK_RETRY_FROM_ROOT) {
        return status::OK_RETRY_FROM_ROOT;
    }
    if (check_status == status::OK_RETRY_AFTER_FB) {
        goto retry; // NOLINT
    }

    // it reaches right endpoint of entire tree.
    if (next == nullptr) { return status::OK_SCAN_END; }

    // it is in scan range and fin scaning this border node.
    *target = next;
    v_at_fb = next_version;
    return status::OK_SCAN_CONTINUE;
}

} // namespace yakushima
