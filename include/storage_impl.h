#pragma once

#include "interface_get.h"
#include "interface_put.h"
#include "interface_remove.h"
#include "storage.h"
#include "tree_instance.h"

namespace yakushima {

// begin - forward declaration
[[maybe_unused]] static status enter(Token& token);// NOLINT
[[maybe_unused]] static status leave(Token token); // NOLINT
template<class ValueType>
[[maybe_unused]] static std::pair<ValueType*, std::size_t> get(tree_instance* ti, std::string_view key_view);
[[maybe_unused]] static status remove(Token token, tree_instance* ti, std::string_view key_view);// NOLINT
// end - forward declaration

status storage::create_storage(std::string_view storage_name) {// NOLINT
    tree_instance new_instance;
    status ret_st{put(get_storages(), storage_name, &new_instance)};
    return ret_st;
}

status storage::delete_storage(std::string_view storage_name) {// NOLINT
    Token token{};
    while (status::OK != enter(token)) _mm_pause();
    // search storage
    auto ret = get<tree_instance>(get_storages(), storage_name);
    if (ret.first == nullptr) {
        leave(token);
        return status::WARN_NOT_EXIST;
    }
    // try remove the storage.
    status ret_st{remove(token, get_storages(), storage_name)};
    if (ret_st == status::OK) {
        base_node* tables_root = ret.first->load_root_ptr();
        if (tables_root != nullptr) {
            tables_root->destroy();
            delete tables_root;// NOLINT
            ret.first->store_root_ptr(nullptr);
        }
        leave(token);
        return status::OK;
    }
    leave(token);
    return status::WARN_CONCURRENT_OPERATIONS;
}

status storage::find_storage(std::string_view storage_name, tree_instance** found_storage) {// NOLINT
    auto ret = get<tree_instance>(get_storages(), storage_name);
    *found_storage = ret.first;
    if (ret.first == nullptr) {
        return status::WARN_NOT_EXIST;
    }
    return status::OK;
}

}// namespace yakushima