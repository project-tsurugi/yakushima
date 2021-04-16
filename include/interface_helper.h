#pragma once

#include "interface_scan.h"
#include "kvs.h"
#include "manager_thread.h"
#include "storage.h"

namespace yakushima {

[[maybe_unused]] static status enter(Token& token) {
    return gc_info_table::assign_gc_info(token);
}

[[maybe_unused]] static status leave(Token token) {
    return gc_info_table::leave_gc_info<interior_node, border_node>(token);
}

[[maybe_unused]] static status destroy() {
    if (storage::get_storages()->empty()) {
        return status::OK_ROOT_IS_NULL;
    }
    std::vector<std::tuple<std::string, tree_instance*, std::size_t>> tuple_list;
    scan(storage::get_storages(), "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_list, nullptr, 0);
    for (auto&& elem : tuple_list) {
        base_node* root = std::get<1>(elem)->load_root_ptr();
        if (root == nullptr) continue;
        root->destroy();
        delete root;// NOLINT
        std::get<1>(elem)->store_root_ptr(nullptr);
    }
    base_node* tables_root = storage::get_storages()->load_root_ptr();
    if (tables_root != nullptr) {
        tables_root->destroy();
        delete tables_root;// NOLINT
        storage::get_storages()->store_root_ptr(nullptr);
    }
    return status::OK_DESTROY_ALL;
}

[[maybe_unused]] static void init() {
    /**
     * initialize thread information table (kThreadInfoTable)
     */
    gc_info_table::init();
    epoch_manager::invoke_epoch_thread();
}

[[maybe_unused]] static void fin() {
    destroy();
    epoch_manager::set_epoch_thread_end();
    epoch_manager::join_epoch_thread();
    gc_container::fin<interior_node, border_node>();
}

}// namespace yakushima