/**
 * @file storage.h
 */

#pragma once

#include <mutex>
#include <string_view>
#include <vector>

#include "base_node.h"
#include "tree_instance.h"

namespace yakushima {

class storage {
public:
    static inline status
    create_storage(std::string_view storage_name); // NOLINT

    static inline status
    delete_storage(std::string_view storage_name); // NOLINT

    static inline status find_storage(std::string_view storage_name,
                                      tree_instance** found_storage); // NOLINT

    static inline tree_instance* get_storages() { return &storages_; }

    static inline status list_storages(
            std::vector<std::pair<std::string, tree_instance*>>& out); // NOLINT

private:
    static inline tree_instance storages_; // NOLINT
};

} // namespace yakushima