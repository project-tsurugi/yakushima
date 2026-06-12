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

[[maybe_unused]] static void mem_usage_display(const memory_usage_stack& mem_stat) {
    auto str_rate = [](std::size_t numerator, std::size_t denominator) -> std::string {
        if (denominator == 0) { return {}; }
        std::ostringstream ss;
        ss << "(" << (numerator * 100ULL / denominator) << "%)";
        return ss.str();
    };
    for (std::size_t l = 0; l < mem_stat.size(); l++) {
        const auto& ls = mem_stat[l];
        std::ostringstream ss;
        ss << "L" << l
           << ": bt_count=" << ls.bt_count
           << ", in_count=" << ls.in_count;
        if (ls.in_count != 0) {
            ss << ", in_allocated_mem=" << ls.in_allocated_mem
               << ", in_used_key=" << ls.in_used_key
               << " " << str_rate(ls.in_used_key, ls.in_count * interior_node::child_length);
        }
        ss << ", bn_count=" << ls.bn_count
           << ", bn_allocated_mem=" << ls.bn_allocated_mem
           << ", bn_used_key=" << ls.bn_used_key;
        if (ls.bn_count != 0) { // B+-tree must have at least one border node, but just in case
            ss << " " << str_rate(ls.bn_used_key, ls.bn_count * key_slice_length);
        }
        ss << ", iv_count=" << ls.iv_count
           << ", vv_count=" << ls.vv_count
           << ", vv_allocated_mem=" << ls.vv_allocated_mem;
        LOG(INFO) << ss.str();
    }
}

[[maybe_unused]] static void mem_usage_display_all() {
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
        mem_usage_display(mem_stat);
    }
}

} // namespace yakushima
