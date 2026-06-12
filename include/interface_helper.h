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
    return thread_info_table::leave_thread_info(token);
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

[[maybe_unused]] static memory_usage_stack
mem_usage(std::string_view storage_name) {
    memory_usage_stack mem_stat{};
    tree_instance* ti{};
    if (status::OK == storage::find_storage(storage_name, &ti)) {
        ti->load_root_ptr()->mem_usage(0, mem_stat);
    }
    return mem_stat;
}

[[maybe_unused]] static void
mem_usage_all() {
    auto str_for_print = [](std::string_view name) {
        if (std::all_of(name.begin(), name.end(), [](unsigned char c){return std::isprint(c);})) {
            return "'" + std::string(name) + "'";
        }
        std::ostringstream ss{};
        ss << std::hex;
        bool first{true};
        for (auto c : name) {
            if (first) { first = false; }
            else { ss << '-'; }
            ss << std::setw(2) << std::setfill('0')
               << static_cast<unsigned int>(static_cast<unsigned char>(c));
        }
        return ss.str();
    };
    std::vector<std::pair<std::string, tree_instance*>> st_list{};
    auto rc = list_storages(st_list);
    if (rc != status::OK) { return; }
    for (auto& st : st_list) {
        memory_usage_stack mem_stat{};
        st.second->load_root_ptr()->mem_usage(0, mem_stat);
        LOG(INFO) << "mem_usage: storage " << st.second << "(" << str_for_print(st.first) << ")";
        for (std::size_t i = 0; i < mem_stat.size(); i++) {
            auto& u = mem_stat[i];
            std::ostringstream ss;
            ss << "L" << i
               << ": i_node_count=" << u.i_node_count
               << ", i_allocated_mem=" << u.i_allocated_mem
               << ", i_used_key=" << u.i_used_key;
            if (u.i_node_count != 0) { ss << " (" << (u.i_used_key * 100ULL / u.i_node_count / 16ULL) << "%)"; }
            ss << ", b_node_count=" << u.b_node_count
               << ", b_allocated_mem=" << u.b_allocated_mem
               << ", b_used_key=" << u.b_used_key;
            if (u.b_node_count != 0) { ss << " (" << (u.b_used_key * 100ULL / u.b_node_count / 15ULL) << "%)"; }
            ss << ", lv_count=" << u.lv_count;
            LOG(INFO) << ss.str();
        }
    }
}

} // namespace yakushima
