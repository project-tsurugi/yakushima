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
    destroy_manager manager{};
    if (storage::get_storages()->empty()) { return status::OK_ROOT_IS_NULL; }
    std::vector<std::tuple<std::string, tree_instance*, std::size_t>>
            tuple_list;
    scan(storage::get_storages(), "", scan_endpoint::INF, "",
         scan_endpoint::INF, tuple_list, nullptr, 0);
    std::vector<std::unique_ptr<destroy_barrier>> barriers{};
    for (auto&& elem : tuple_list) {
        base_node* root = std::get<1>(elem)->load_root_ptr();
        if (root == nullptr) { continue; }
        barriers.emplace_back(std::make_unique<destroy_barrier>([root, elem](){
            delete root; // NOLINT
            std::get<1>(elem)->store_root_ptr(nullptr);
        }));
        destroy_barrier& barrier = *barriers.back();
        barrier.begin();
        manager.put([root, elem, &manager, &barrier](){
            root->destroy(manager, barrier);
            barrier.end();
        });
    }
    for(auto&& e: barriers) {
        e->wait();
    }

    base_node* tables_root = storage::get_storages()->load_root_ptr();
    if (tables_root != nullptr) {
        destroy_barrier barrier([tables_root](){
            delete tables_root; // NOLINT
            storage::get_storages()->store_root_ptr(nullptr);
        });
        barrier.begin();
        manager.put([tables_root, &manager, &barrier](){
            tables_root->destroy(manager, barrier);
            barrier.end();
        });
        barrier.wait();
    }
    return status::OK_DESTROY_ALL;
}

} // namespace yakushima
