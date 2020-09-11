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
static status scan_border(border_node** target, std::string_view l_key, scan_endpoint l_end, std::string_view r_key,
                          scan_endpoint r_end, std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
                          node_version64_body &v_at_fetch_lv,
                          std::vector<std::pair<node_version64_body, node_version64*>>* node_version_vec);

template<class ValueType>
status scan_check_retry(border_node* const bn, node_version64_body &v_at_fetch_lv,
                        std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
                        const std::size_t tuple_pushed_num,
                        std::vector<std::pair<node_version64_body, node_version64*>>* const node_version_vec) {
    node_version64_body check = bn->get_stable_version();
    if (check != v_at_fetch_lv) {
        if (tuple_pushed_num != 0) {
            tuple_list.erase(tuple_list.end() - tuple_pushed_num, tuple_list.end());
            if (node_version_vec != nullptr) {
                node_version_vec->erase(node_version_vec->end() - tuple_pushed_num, node_version_vec->end());
            }
        }
        if (check.get_vsplit() != v_at_fetch_lv.get_vsplit() ||
            check.get_deleted()) {
            return status::OK_RETRY_FROM_ROOT;
        }
        v_at_fetch_lv = check;
        return status::OK_RETRY_FETCH_LV;
    }
    return status::OK;
}

template<class ValueType>
status scan_check_retry(border_node* const bn, node_version64_body &v_at_fetch_lv) {
    std::vector<std::pair<ValueType*, std::size_t>> tuple_list;
    std::size_t dummy_ctr(0);
    return scan_check_retry<ValueType>(bn, v_at_fetch_lv, tuple_list, dummy_ctr, nullptr);
}

template<class ValueType>
static status
scan(base_node* const root, const std::string_view l_key, const scan_endpoint l_end, const std::string_view r_key,
     const scan_endpoint r_end, std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>* const node_version_vec) {
retry:
    if (root->get_version_deleted() || !root->get_version_root()) {
        return status::OK_RETRY_FROM_ROOT;
    }

    std::tuple<border_node*, node_version64_body> node_and_v;
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    status check_status;
    key_slice_type ks{0};
    key_length_type kl; // NOLINT
    if (l_key.size() > sizeof(key_slice_type)) {
        memcpy(&ks, l_key.data(), sizeof(key_slice_type));
        kl = sizeof(key_slice_type);
    } else {
        if (!l_key.empty()) {
            memcpy(&ks, l_key.data(), l_key.size());
        }
        kl = static_cast<key_length_type>(l_key.size());
    }
    node_and_v = find_border(root, ks, kl, check_status);
    if (check_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        return status::OK_RETRY_FETCH_LV;
    }
    border_node* bn(std::get<tuple_node_index>(node_and_v));
    node_version64_body check_v = std::get<tuple_v_index>(node_and_v);

    for (;;) {
        check_status = scan_border<ValueType>(&bn, l_key, l_end, r_key, r_end, tuple_list, check_v,
                                              node_version_vec);
        if (check_status == status::OK_SCAN_END) {
            return status::OK;
        }
        if (check_status == status::OK_SCAN_CONTINUE) {
            continue;
        }
        if (check_status == status::OK_RETRY_FETCH_LV) {
            node_version64_body re_check_v = bn->get_stable_version();
            if (check_v.get_vsplit() != re_check_v.get_vsplit() ||
                re_check_v.get_deleted()) {
                return status::OK_RETRY_FETCH_LV;
            }
            if (check_v.get_vinsert_delete() != re_check_v.get_vinsert_delete()) {
                check_v = re_check_v;
                continue;
            }
        } else if (check_status == status::OK_RETRY_FROM_ROOT) {
            goto retry; // NOLINT
        }
    }
}

template<class ValueType>
static status scan_border(border_node** const target, const std::string_view l_key, const scan_endpoint l_end,
                          const std::string_view r_key, const scan_endpoint r_end,
                          std::vector<std::pair<ValueType*, std::size_t>> &tuple_list,
                          node_version64_body &v_at_fetch_lv,
                          std::vector<std::pair<node_version64_body, node_version64*>>* const node_version_vec) {
retry:
    std::size_t tuple_pushed_num{0};
    border_node* bn = *target;
    border_node* next = bn->get_next();
    permutation perm(bn->get_permutation().get_body());
    for (std::size_t i = 0; i < perm.get_cnk(); ++i) {
        std::size_t index = perm.get_index_of_rank(i);
        key_slice_type ks = bn->get_key_slice_at(index);
        key_length_type kl = bn->get_key_length_at(index);
        link_or_value* lv = bn->get_lv_at(index);
        void* vp = lv->get_v_or_vp_();
        std::size_t vsize = lv->get_value_length();
        base_node* next_layer = lv->get_next_layer();
        node_version64* node_version_ptr = bn->get_version_ptr();
        status check_status = scan_check_retry(bn, v_at_fetch_lv, tuple_list, tuple_pushed_num, node_version_vec);
        if (check_status == status::OK_RETRY_FROM_ROOT) {
            return status::OK_RETRY_FROM_ROOT;
        }
        if (check_status == status::OK_RETRY_FETCH_LV) {
            goto retry; // NOLINT
        }
        key_slice_type new_key_slice_one{1};
        /**
         * Given an 8-byte length key_view and next_exclusive == false, set key_slice = 1 / key_length = 1 /
         * next_exclusive = true to distinguish it from inf (key_view ("")) when descending to the next layer.
         */
        if (kl > sizeof(key_slice_type)) {
            std::string_view arg_l_key;
            scan_endpoint arg_l_end;
            if (l_end == scan_endpoint::INF) {
                arg_l_key = "";
                arg_l_end = scan_endpoint::INF;
            } else {
                key_slice_type l_key_slice{};
                memcpy(&l_key_slice, l_key.data(),
                       l_key.size() < sizeof(key_slice_type) ? l_key.size() : sizeof(key_slice_type));
                int ret_cmp = memcmp(&l_key_slice, &ks, sizeof(key_slice_type));
                if (ret_cmp < 0) {
                    arg_l_key = "";
                    arg_l_end = scan_endpoint::INF;
                } else if (ret_cmp == 0) {
                    arg_l_key = l_key;
                    arg_l_key.remove_prefix(sizeof(key_slice_type));
                    arg_l_end = l_end;
                } else {
                    continue;
                }
            }
            std::string_view arg_r_key;
            scan_endpoint arg_r_end;
            if (r_end == scan_endpoint::INF) {
                arg_r_key = "";
                arg_r_end = scan_endpoint::INF;
            } else {
                key_slice_type r_key_slice{};
                memcpy(&r_key_slice, r_key.data(),
                       r_key.size() < sizeof(key_slice_type) ? r_key.size() : sizeof(key_slice_type));
                int ret_cmp = memcmp(&r_key_slice, &ks, sizeof(key_slice_type));
                if (ret_cmp < 0) {
                    return status::OK_SCAN_END;
                }
                if (ret_cmp == 0) {
                    arg_r_key = r_key;
                    arg_r_key.remove_prefix(sizeof(key_slice_type));
                    arg_r_end = r_end;
                } else {
                    arg_r_key = "";
                    arg_r_end = scan_endpoint::INF;
                }
            }
            check_status = scan(next_layer, arg_l_key, arg_l_end, arg_r_key, arg_r_end, tuple_list, node_version_vec);
            if (check_status != status::OK) {
                goto retry; // NOLINT
            }
        } else {
            if (l_end == scan_endpoint::INF && r_end == scan_endpoint::INF) {
                // all range
                tuple_list.emplace_back(std::make_pair(reinterpret_cast<ValueType*>(vp), vsize)); // NOLINT
                if (node_version_vec != nullptr) {
                    node_version_vec->emplace_back(std::make_pair(v_at_fetch_lv, node_version_ptr));
                }
                ++tuple_pushed_num;
                continue;
            }
            // not all range
            key_slice_type l_key_slice{};
            memcpy(&l_key_slice, l_key.data(),
                   l_key.size() < sizeof(key_slice_type) ? l_key.size() : sizeof(key_slice_type));
            int l_cmp = memcmp(&l_key_slice, &ks, sizeof(key_slice_type));
            if (l_cmp > 0 ||
                (l_cmp == 0 && (l_key.size() > kl || (l_key.size() == kl && l_end == scan_endpoint::EXCLUSIVE)))) {
                continue;
            }
            // pass left endpoint.
            key_slice_type r_key_slice{};
            memcpy(&r_key_slice, r_key.data(),
                   r_key.size() < sizeof(key_slice_type) ? r_key.size() : sizeof(key_slice_type));
            int r_cmp = memcmp(&r_key_slice, &ks, sizeof(key_slice_type));
            if (r_cmp > 0 ||
                (r_cmp == 0 && (r_key.size() < kl || (r_key.size() == kl && r_end == scan_endpoint::INCLUSIVE)))) {
                tuple_list.emplace_back(std::make_pair(reinterpret_cast<ValueType*>(vp), vsize)); // NOLINT
                if (node_version_vec != nullptr) {
                    node_version_vec->emplace_back(std::make_pair(v_at_fetch_lv, node_version_ptr));
                }
                ++tuple_pushed_num;
                continue;
            }
            // pass right endpoint.
            return status::OK_SCAN_END;
        }
    }

    if (next == nullptr) {
        return status::OK_SCAN_END;
    }
    *target = next;
    v_at_fetch_lv = next->get_stable_version();
    return status::OK_SCAN_CONTINUE;
}

} // namespace yakushima