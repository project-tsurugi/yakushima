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
    if (storage::get_storages()->empty()) {
        return status::OK_ROOT_IS_NULL;
    }
    std::vector<std::tuple<std::string, tree_instance*, std::size_t>> tuple_list;
    scan(storage::get_storages(), "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_list, nullptr, 0);
    std::vector<std::thread> th_vc;
    th_vc.reserve(tuple_list.size());
    for (auto&& elem : tuple_list) {
        auto process = [&elem] {
            base_node* root = std::get<1>(elem)->load_root_ptr();
            if (root == nullptr) return;
            root->destroy();
            delete root; // NOLINT
            std::get<1>(elem)->store_root_ptr(nullptr);
        };
        if (destroy_manager::check_room()) {
            th_vc.emplace_back(process);
        } else {
            process();
        }
    }
    for (auto&& th : th_vc) {
        th.join();
        destroy_manager::return_room();
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