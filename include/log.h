#pragma once

#include <iostream>

#include "glog/logging.h"

namespace yakushima {

constexpr static std::string_view log_location_prefix = "/:yakushima ";

/**
 * @brief logging level constant for errors
 */
static constexpr std::int32_t log_error = 10;

/**
 * @brief logging level constant for warnings
 */
static constexpr std::int32_t log_warning = 20;

/**
 * @brief logging level constant for information
 */
static constexpr std::int32_t log_info = 30;

/**
 * @brief logging level constant for debug information
 */
static constexpr std::int32_t log_debug = 40;

/**
 * @brief logging level constant for traces
 */
static constexpr std::int32_t log_trace = 50;

#define yakushima_log_entry                                                    \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "-->" // NOLINT

#define yakushima_log_exit                                                     \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "<--" // NOLINT

} // namespace yakushima