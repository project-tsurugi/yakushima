#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <string_view>

#include "base_node.h"

namespace yakushima {

class storage {
public:
    static status create_storage(std::string_view table_name);

    static status delete_storage(std::string_view table_name);

    static status find_storage(std::string_view table_name, std::atomic<base_node*>** found_storage);

private:
    static inline std::map<std::string, std::atomic<base_node*>> storages; // NOLINT
};

}// namespace yakushima