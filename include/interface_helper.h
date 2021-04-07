#pragma once

#include "kvs.h"
#include "manager_thread.h"

namespace yakushima {

[[maybe_unused]] static status enter(Token &token) {
    return gc_info_table::assign_gc_info(token);
}

[[maybe_unused]] static status leave(Token token) {
    return gc_info_table::leave_gc_info<interior_node, border_node>(token);
}

[[maybe_unused]] static status destroy() {
    base_node* root{base_node::get_root_ptr()};
    if (root == nullptr) return status::OK_ROOT_IS_NULL;
    base_node::get_root_ptr()->destroy();
    delete base_node::get_root_ptr(); // NOLINT
    base_node::set_root(nullptr);
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

}