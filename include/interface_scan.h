/**
 * @file interface_scan.h
 */

#pragma once

#include "kvs.h"
#include "log.h"
#include "scan_helper.h"
#include "storage.h"
#include "tree_instance.h"

#include "glog/logging.h"

namespace yakushima {

template<class ValueType>
[[maybe_unused]] static status
scan(tree_instance* ti, std::string_view l_key, scan_endpoint l_end,
     std::string_view r_key, scan_endpoint r_end,
     std::vector<std::tuple<std::string, ValueType*, std::size_t>>& tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>*
             node_version_vec,
     std::size_t max_size) {
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
    if (l_key == r_key && (l_end == scan_endpoint::EXCLUSIVE ||
                           r_end == scan_endpoint::EXCLUSIVE)) {
        return status::ERR_BAD_USAGE;
    }

    // clear out parameter
    tuple_list.clear();
    if (node_version_vec != nullptr) {
        // this out parameter is used.
        node_version_vec->clear();
    }

retry_from_root:
    std::string key_prefix;
    base_node* root = ti->load_root_ptr();

    if (root == nullptr) { return status::OK_ROOT_IS_NULL; }
    std::string_view traverse_key_view{l_key};

    /**
      * prepare key_slice
      */
    key_slice_type key_slice(0);
    auto key_slice_length =
            static_cast<key_length_type>(traverse_key_view.size());
    if (traverse_key_view.size() > sizeof(key_slice_type)) {
        memcpy(&key_slice, traverse_key_view.data(), sizeof(key_slice_type));
    } else {
        if (!traverse_key_view.empty()) {
            memcpy(&key_slice, traverse_key_view.data(),
                   traverse_key_view.size());
        }
    }
    /**
      * traverse tree to border node.
      */
    status check_status{status::OK};
    std::tuple<border_node*, node_version64_body> node_and_v =
            find_border(root, key_slice, key_slice_length, check_status);
    if (check_status == status::WARN_RETRY_FROM_ROOT_OF_ALL) {
        /**
          * @a root is the root node of the some layer, but it was deleted.
          * So it must retry from root of the all tree.
          */
        goto retry_from_root; // NOLINT
    }
    constexpr std::size_t tuple_node_index = 0;
    constexpr std::size_t tuple_v_index = 1;
    border_node* target_border = std::get<tuple_node_index>(node_and_v);

    // here, it decides to scan from this nodes.
    for (;;) {
        if (std::get<1>(node_and_v).get_deleted() &&
            std::get<1>(node_and_v).get_root()) {
            if (node_version_vec != nullptr) {
                // log full scan empty result
                node_version_vec->clear();
                node_version_vec->emplace_back(
                        std::make_pair(std::get<1>(node_and_v),
                                       target_border->get_version_ptr()));
            }
            return status::OK;
        }
        check_status = scan_border<ValueType>(
                &target_border, traverse_key_view, l_end, r_key, r_end,
                tuple_list, std::get<tuple_v_index>(node_and_v),
                node_version_vec, key_prefix, max_size);
        if (check_status == status::OK_SCAN_END) { return status::OK; }
        if (check_status == status::OK_SCAN_CONTINUE) { continue; }
        if (check_status == status::OK_RETRY_FROM_ROOT) {
            goto retry_from_root; // NOLINT
        } else {
            // unreachable
            LOG(ERROR) << log_location_prefix;
        }
    }
}

template<class ValueType>
[[maybe_unused]] static status
scan(std::string_view storage_name, std::string_view l_key, // NOLINT
     scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
     std::vector<std::tuple<std::string, ValueType*, std::size_t>>& tuple_list,
     std::vector<std::pair<node_version64_body, node_version64*>>*
             node_version_vec = nullptr,
     std::size_t max_size = 0) {
    // check storage
    tree_instance* ti{};
    if (storage::find_storage(storage_name, &ti) != status::OK) {
        return status::WARN_STORAGE_NOT_EXIST;
    }
    return scan(ti, l_key, l_end, r_key, r_end, tuple_list, node_version_vec,
                max_size);
}

} // namespace yakushima