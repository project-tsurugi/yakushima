#pragma once

#include <mutex>
#include <string_view>
#include <vector>

#include "base_node.h"
#include "tree_instance.h"

namespace yakushima {

class storage {
public:
    static status create_storage(std::string_view storage_name);// NOLINT

    static status delete_storage(std::string_view storage_name);// NOLINT

    static status find_storage(std::string_view storage_name, tree_instance** found_storage);// NOLINT

    static tree_instance* get_storages() { return &storages_; }

    static status list_storages(std::vector<std::pair<std::string, tree_instance*>>& out);// NOLINT

private:
    static inline tree_instance storages_;// NOLINT
};

}// namespace yakushima