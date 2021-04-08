#pragma once

#include "storage.h"

namespace yakushima {

status storage::create_storage(std::string_view storage_name) { // NOLINT
    auto elem = storages_.find(std::string{storage_name});
    if (elem == storages_.end()) {
        storages_.insert(std::make_pair(storage_name, nullptr));
        return status::OK;
    }
    return status::WARN_EXIST;
}

status storage::delete_storage(std::string_view storage_name) { // NOLINT
    auto ret = storages_.erase(std::string{storage_name});
    if (ret == 0) {
        return status::WARN_NOT_EXIST;
    }
    return status::OK;
}

status storage::find_storage(std::string_view storage_name, std::atomic<base_node*>** found_storage) { // NOLINT
    auto elem = storages_.find(std::string{storage_name});
    if (elem == storages_.end()) {
        return status::WARN_NOT_EXIST;
    }
    *found_storage = &elem->second;
    return status::OK;
}

}// namespace yakushima