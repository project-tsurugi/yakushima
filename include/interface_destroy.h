/**
 * @file destroy.h
 */

#pragma once

#include <atomic>
#include <tuple>
#include <vector>

#include "destroy_manager.h"
#include "interface_scan.h"
#include "kvs.h"
#include "storage.h"
#include "storage_impl.h"

namespace yakushima {

[[maybe_unused]] static status destroy() {
    if (storage::get_storages()->empty()) { return status::OK_ROOT_IS_NULL; }
    std::vector<tree_instance*> tuple_list;
    scan_root<tree_instance, false, false>(
            storage::get_storages(), "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_list, nullptr, 0);
    for (auto&& elem : tuple_list) {
        base_node* root = elem->load_root_ptr();
        if (root == nullptr) { continue; }
        root->destroy();
        delete root; // NOLINT
        elem->store_root_ptr(nullptr);
    }

    base_node* tables_root = storage::get_storages()->load_root_ptr();
    if (tables_root != nullptr) {
        tables_root->destroy();
        delete tables_root; // NOLINT
        storage::get_storages()->store_root_ptr(nullptr);
    }
    return status::OK_DESTROY_ALL;
}

} // namespace yakushima
