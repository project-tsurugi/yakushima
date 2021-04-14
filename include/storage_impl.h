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
    while (status::OK != enter(token));
    status ret_st{remove(token, get_storages(), storage_name)};
    leave(token);
    return ret_st;
}

status storage::find_storage(std::string_view storage_name, tree_instance** found_storage) {// NOLINT
    auto ret = get<tree_instance>(get_storages(), storage_name);
    status ret_st;
    if (ret.first == nullptr) {
        ret_st = status::WARN_NOT_EXIST;
    } else {
        *found_storage = ret.first;
        ret_st = status::OK;
    }
    return ret_st;
}

}// namespace yakushima