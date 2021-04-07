#pragma once

#include "kvs.h"
#include "scan_helper.h"

namespace yakushima {

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
     * 1 : l_key == r_key && (
     * (l_end == scan_endpoint::INCLUSIVE || r_end == scan_endpoint::INCLUSIVE) ||
     * (l_end == scan_endpoint::INCLUSIVE || r_end == scan_endpoint::INF) ||
     * (l_end == scan_endpoint::INF || r_end == scan_endpoint::INCLUSIVE) ||
     * )
     * , only one point that matches the endpoint can be scanned.
     * 2 : l_key == r_key &&  l_end == r_end == scan_endpoint::INF, all range.
     */
    if (l_key == r_key && (l_end == scan_endpoint::EXCLUSIVE || r_end == scan_endpoint::EXCLUSIVE)) {
        return status::ERR_BAD_USAGE;
    }

    tuple_list.clear();
    if (node_version_vec != nullptr) {
        node_version_vec->clear();
    }
retry_from_root:
    std::string key_prefix;
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
    [[maybe_unused]] std::size_t ks{0}; // NOLINT
    [[maybe_unused]] std::size_t kl{0}; // NOLINT
    [[maybe_unused]] base_node* next_layer{nullptr}; // NOLINT
    if (lv_ptr != nullptr) {
        ks = target_border->get_key_slice_at(lv_pos);
        kl = target_border->get_key_length_at(lv_pos);
        next_layer = lv_ptr->get_next_layer();
    }

    check_status = scan_check_retry<ValueType>(target_border, v_at_fetch_lv);
    if (check_status == status::OK_RETRY_FROM_ROOT) goto retry_from_root; // NOLINT
    else if (check_status == status::OK_RETRY_FETCH_LV) goto retry_fetch_lv; // NOLINT

    if (lv_ptr != nullptr &&
        kl > sizeof(key_slice_type)) {
        traverse_key_view.remove_prefix(sizeof(key_slice_type));
        key_prefix.append(reinterpret_cast<char*>(&ks), sizeof(key_slice_type)); // NOLINT
        root = next_layer;
        goto retry_find_border; // NOLINT
    }
    // here, it decides to scan from this nodes.
    for (;;) {
        check_status = scan_border<ValueType>(&target_border, traverse_key_view, l_end, r_key, r_end,
                                              tuple_list, v_at_fetch_lv, node_version_vec, key_prefix);
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

}