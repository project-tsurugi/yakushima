/**
 * @file scheme.h
 */

#pragma once

#include <cstdint>
#include <cstdlib>
#include <string_view>

#include "log.h"

#include "glog/logging.h"

namespace yakushima {

/**
 * @brief Session token
 */
using Token = void*;

using key_slice_type = std::uint64_t;
static constexpr std::size_t key_slice_length = 15;
/**
 * key_length_type is used at permutation.h, border_node.h.
 * To avoid circular reference at there, declare here.
 */
using key_length_type = std::uint8_t;
using value_length_type = std::size_t;
using value_align_type = std::align_val_t;

enum class status : std::int32_t {
    /**
   * @brief Warning of mistaking usage.
   */
    WARN_BAD_USAGE,
    WARN_CONCURRENT_OPERATIONS,
    WARN_EXIST,
    WARN_NOT_EXIST,
    /**
   * @brief warning
   */
    WARN_INVALID_TOKEN,
    /**
   * @brief Warning
   * @details The find_border function tells that it must retry from root of all tree.
   */
    WARN_RETRY_FROM_ROOT_OF_ALL,
    /**
   * @brief warning
   * @details The target storage of operations is not found.
   */
    WARN_STORAGE_NOT_EXIST,
    /**
   * @brief Warning
   * @details (assign_gc_info) The maximum number of sessions is already up and running.
   */
    WARN_MAX_SESSIONS,
    /**
   * @brief Warning
   * @details Masstree originally has a unique key constraint.
   * todo (optional): This constraint is removed.
   */
    WARN_UNIQUE_RESTRICTION,
    /**
   * @brief success status
   */
    OK,
    /**
   * @brief (destroy) It destroys existing all trees.
   */
    OK_DESTROY_ALL,
    /**
   * @brief (destroy) It destroys xxx.
   */
    OK_DESTROY_BORDER,
    /**
   * @brief (destroy) It destroys xxx.
   */
    OK_DESTROY_INTERIOR,
    /**
   * @brief Warning
   * @details (get/delete) No corresponding value in this storage engine.
   */
    OK_NOT_FOUND,
    OK_RETRY_FETCH_LV,
    OK_RETRY_AFTER_FB,
    OK_RETRY_FROM_ROOT,
    OK_ROOT_IS_DELETED,
    /**
   * @brief
   * (destroy) Root is nullptr and it could not destroy.
   * (remove) No existing tree.
   */
    OK_ROOT_IS_NULL,
    OK_SCAN_CONTINUE,
    OK_SCAN_END,
    ERR_ARGUMENT,
    ERR_BAD_USAGE,
    ERR_BOUNDARY,
    /**
     * @brief fatal error
     * 
     */
    ERR_FATAL,
    /**
   * @brief root is not both interior and border.
   */
    ERR_UNKNOWN_ROOT,
};

inline constexpr std::string_view to_string_view(const status value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case status::WARN_BAD_USAGE:
            return "WARN_BAD_USAGE"sv;
        case status::WARN_CONCURRENT_OPERATIONS:
            return "WARN_CONCURRENT_OPERATIONS"sv;
        case status::WARN_EXIST:
            return "WARN_EXIST"sv;
        case status::WARN_NOT_EXIST:
            return "WARN_NOT_EXIST"sv;
        case status::WARN_INVALID_TOKEN:
            return "WARN_INVALID_TOKEN"sv;
        case status::WARN_MAX_SESSIONS:
            return "WARN_MAX_SESSIONS"sv;
        case status::WARN_RETRY_FROM_ROOT_OF_ALL:
            return "WARN_RETRY_FROM_ROOT_OF_ALL"sv;
        case status::WARN_STORAGE_NOT_EXIST:
            return "WARN_STORAGE_NOT_EXIST"sv;
        case status::WARN_UNIQUE_RESTRICTION:
            return "WARN_UNIQUE_RESTRICTION"sv;
        case status::OK:
            return "OK"sv;
        case status::OK_DESTROY_ALL:
            return "OK_DESTROY_ALL"sv;
        case status::OK_DESTROY_BORDER:
            return "OK_DESTROY_BORDER"sv;
        case status::OK_DESTROY_INTERIOR:
            return "OK_DESTROY_INTERIOR"sv;
        case status::OK_NOT_FOUND:
            return "OK_NOT_FOUND"sv;
        case status::OK_ROOT_IS_NULL:
            return "OK_ROOT_IS_NULL"sv;
        case status::OK_RETRY_AFTER_FB:
            return "OK_RETRY_AFTER_FB"sv;
        case status::OK_RETRY_FETCH_LV:
            return "OK_RETRY_FETCH_LV"sv;
        case status::OK_RETRY_FROM_ROOT:
            return "OK_RETRY_FROM_ROOT"sv;
        case status::OK_ROOT_IS_DELETED:
            return "OK_ROOT_IS_DELETED"sv;
        case status::OK_SCAN_CONTINUE:
            return "OK_SCAN_CONTINUE"sv;
        case status::OK_SCAN_END:
            return "OK_SCAN_END"sv;
        case status::ERR_ARGUMENT:
            return "ERR_ARGUMENT"sv;
        case status::ERR_BAD_USAGE:
            return "ERR_BAD_USAGE"sv;
        case status::ERR_BOUNDARY:
            return "ERR_BOUNDARY"sv;
        case status::ERR_FATAL:
            return "ERR_FATAL"sv;
        case status::ERR_UNKNOWN_ROOT:
            return "ERR_UNKNOWN_ROOT"sv;
    }
    LOG(ERROR) << log_location_prefix;
    return ""sv;
}

inline std::ostream& operator<<(std::ostream& out, const status value) {
    return out << to_string_view(value);
}

/**
 * @brief Information about scan's endpoints.
 */
enum class scan_endpoint : char {
    /**
   * @details Excludes those that match the key specified for the endpoint.
   */
    EXCLUSIVE,
    /**
   * @details Includes those that match the key specified for the endpoint.
   */
    INCLUSIVE,
    /**
   * @details Do not limit the endpoints.
   */
    INF,
};

} // namespace yakushima