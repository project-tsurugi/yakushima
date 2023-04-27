/**
 * @file bench/include/memory.h
 */

#pragma once

#include <sys/resource.h>

#include <cstdio>
#include <ctime>
#include <iostream>

#include "glog/logging.h"

namespace yakushima {

[[maybe_unused]] static std::size_t getRusageRUMaxrss() {
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) { LOG(ERROR) << "getrusage error"; }
    return r.ru_maxrss; // NOLINT
}

[[maybe_unused]] static void displayRusageRUMaxrss() { // NOLINT
    struct rusage r {};
    if (getrusage(RUSAGE_SELF, &r) != 0) {
        LOG(ERROR) << "getrusage error.";
        return;
    }
    std::size_t maxrss{getRusageRUMaxrss()};
    printf("maxrss:\t%ld kB\n", maxrss); // NOLINT
}

} // namespace yakushima
