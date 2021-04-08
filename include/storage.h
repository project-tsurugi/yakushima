#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string_view>

#include "base_node.h"

namespace yakushima {

class storage {
public:
    static status create_storage(std::string_view storage_name); // NOLINT

    static status delete_storage(std::string_view storage_name); // NOLINT

    static status find_storage(std::string_view storage_name, std::atomic<base_node*>** found_storage); // NOLINT

    static std::map<std::string, std::atomic<base_node*>>& get_storages() { return storages_; }
private:
    static inline std::map<std::string, std::atomic<base_node*>> storages_; // NOLINT
};

}// namespace yakushima