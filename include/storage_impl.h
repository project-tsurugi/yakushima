#pragma once

#include "storage.h"

namespace yakushima {

status storage::create_storage(std::string_view table_name) {
    auto elem = storages.find(table_name);
    if (elem == storages.end()) {
        storages.insert(std::make_pair(table_name, nullptr));
        return status::OK;
    }
    return status::WARN_EXIST;
}

status storage::delete_storage(std::string_view table_name) {
    auto ret = storages.erase(table_name);
    if (ret == 0) {
        return status::WARN_NOT_EXIST;
    }
    return status::OK;
}

status storage::find_storage(std::string_view table_name, std::atomic<base_node*>** found_storage) {
    auto elem = storages.find(table_name);
    if (elem == storages.end()) {
        return status::WARN_NOT_EXIST;
    }
    *found_storage = &elem;
}

}// namespace yakushima