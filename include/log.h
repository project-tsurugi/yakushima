#pragma once

#include <iostream>

#include "glog/logging.h"

namespace yakushima {

constexpr static std::string_view log_location_prefix = "/:yakushima ";

#define yakushima_log_entry                                                    \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "-->"; // NOLINT

#define yakushima_log_exit                                                     \
    VLOG(log_trace) << std::boolalpha << log_location_prefix << "<--"; // NOLINT

} // namespace yakushima