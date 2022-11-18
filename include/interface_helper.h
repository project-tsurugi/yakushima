/**
 * @file interface_helper.h
 */

#pragma once

#include "interface_scan.h"
#include "kvs.h"
#include "manager_thread.h"
#include "storage.h"

namespace yakushima {

[[maybe_unused]] static status enter(Token& token) {
    return thread_info_table::assign_thread_info(token);
}

[[maybe_unused]] static status leave(Token token) {
    return thread_info_table::leave_thread_info<interior_node, border_node>(
            token);
}

[[maybe_unused]] static void init() {
    /**
   * initialize thread information table (kThreadInfoTable)
   */
    thread_info_table::init();
    epoch_manager::invoke_epoch_thread();
    epoch_manager::invoke_gc_thread();
}

[[maybe_unused]] static void fin() {
    destroy();
    epoch_manager::set_epoch_thread_end();
    epoch_manager::set_gc_thread_end();
    epoch_manager::join_epoch_thread();
    epoch_manager::join_gc_thread();
    thread_info_table::fin();
}

} // namespace yakushima