#pragma once

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
    if (storage::get_storages().empty()) {
        return status::OK_ROOT_IS_NULL;
    }
    for (auto&& each_storage : storage::get_storages()) {
        base_node* root{each_storage.second.load(std::memory_order_acquire)};
        if (root == nullptr) continue;
        root->destroy();
        delete root;// NOLINT
        each_storage.second.store(nullptr, std::memory_order_release);
    }
    storage::get_storages().clear();
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