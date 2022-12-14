/**
 * @file storage_impl.h
 */

#pragma once

#include <tuple>

#include "interface_get.h"
#include "interface_put.h"
#include "interface_remove.h"
#include "log.h"
#include "storage.h"
#include "tree_instance.h"

#include "glog/logging.h"

namespace yakushima {

// begin - forward declaration
[[maybe_unused]] static status enter(Token& token);                   // NOLINT
[[maybe_unused]] static status leave(Token token);                    // NOLINT
[[maybe_unused]] static status remove(Token token, tree_instance* ti, // NOLINT
                                      std::string_view key_view);

status storage::create_storage(std::string_view storage_name) { // NOLINT
    // prepare create storage
    tree_instance new_instance;
    border_node* new_border = new border_node(); // NOLINT
    new_border->init_border();
    new_instance.store_root_ptr(new_border);
    Token token{};
    while (status::OK != enter(token)) { _mm_pause(); }

    // try creating storage
    status ret_st{
            put(token, get_storages(), storage_name, &new_instance, true)};

    // cleanup
    auto ret_st_token = leave(token);
    if (ret_st_token != status::OK) {
        LOG(ERROR) << log_location_prefix << ret_st_token;
    }
    if (ret_st != status::OK) {
        delete new_border; // NOLINT
    }

    return ret_st;
}

status storage::delete_storage(std::string_view storage_name) { // NOLINT
    Token token{};
    while (status::OK != enter(token)) { _mm_pause(); }
    // search storage
    std::pair<tree_instance*, std::size_t> ret{};
    auto rc = get<tree_instance>(get_storages(), storage_name, ret);
    if (rc == status::WARN_NOT_EXIST) {
        leave(token);
        return status::WARN_NOT_EXIST;
    }
    // try remove the storage.
    status ret_st{remove(token, get_storages(), storage_name)};
    if (ret_st == status::OK) {
        base_node* tables_root = ret.first->load_root_ptr();
        if (tables_root != nullptr) {
            tables_root->destroy();
            delete tables_root; // NOLINT
            ret.first->store_root_ptr(nullptr);
        }
        leave(token);
        return status::OK;
    }
    leave(token);
    return status::WARN_CONCURRENT_OPERATIONS;
}

status storage::find_storage(std::string_view storage_name,
                             tree_instance** found_storage) { // NOLINT
    std::pair<tree_instance*, std::size_t> ret{};
    auto rc = get<tree_instance>(get_storages(), storage_name, ret);
    if (rc == status::WARN_NOT_EXIST) { return status::WARN_NOT_EXIST; }
    if (found_storage != nullptr) *found_storage = ret.first;
    return status::OK;
}

status storage::list_storages(
        std::vector<std::pair<std::string, tree_instance*>>& out) { // NOLINT
    out.clear();
    std::vector<std::tuple<std::string, tree_instance*, std::size_t>>
            tuple_list;
    scan(get_storages(), "", scan_endpoint::INF, "", scan_endpoint::INF,
         tuple_list, nullptr, 0);
    if (tuple_list.empty()) { return status::WARN_NOT_EXIST; }
    out.reserve(tuple_list.size());
    for (auto&& elem : tuple_list) {
        out.emplace_back(std::get<0>(elem), std::get<1>(elem));
    }
    return status::OK;
}

} // namespace yakushima