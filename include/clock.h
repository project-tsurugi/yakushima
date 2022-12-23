/**
 * @file clock.h
 * @brief functions about clock.
 */

#pragma once

#include <chrono>
#include <iostream>
#include <thread>

#include "log.h"

#include "glog/logging.h"

namespace yakushima {

[[maybe_unused]] static bool check_clock_span(uint64_t& start, uint64_t& stop,
                                              uint64_t threshold) {
    if (stop < start) { LOG(ERROR) << log_location_prefix; }
    uint64_t diff{stop - start};
    return diff > threshold;
}

[[maybe_unused]] static void sleepMs(size_t ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

} // namespace yakushima
