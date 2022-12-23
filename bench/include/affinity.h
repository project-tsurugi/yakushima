#pragma once

#include <cpuid.h>
#include <sched.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <iostream>

#include "glog/logging.h"

#ifdef YAKUSHIMA_LINUX

[[maybe_unused]] static void set_thread_affinity(const int myid) {
    using namespace std;
    static std::atomic<int> nprocessors(-1);
    int local_nprocessors{nprocessors.load(memory_order_acquire)};
    int desired{};
    for (;;) {
        if (local_nprocessors != -1) { break; }
        desired = sysconf(_SC_NPROCESSORS_CONF); // NOLINT
        if (nprocessors.compare_exchange_strong(local_nprocessors, desired,
                                                memory_order_acq_rel,
                                                memory_order_acquire)) {
            break;
        }
    }

    pid_t pid = syscall(SYS_gettid); // NOLINT
    cpu_set_t cpu_set;

    CPU_ZERO(&cpu_set);
    CPU_SET(myid % local_nprocessors, &cpu_set); // NOLINT

    if (sched_setaffinity(pid, sizeof(cpu_set_t), &cpu_set) != 0) {
        LOG(ERROR);
    }
}

[[maybe_unused]] static void set_thread_affinity(const cpu_set_t id) {
    pid_t pid = syscall(SYS_gettid); // NOLINT

    if (sched_setaffinity(pid, sizeof(cpu_set_t), &id) != 0) { LOG(ERROR); }
}

[[maybe_unused]] static cpu_set_t get_thread_affinity() {
    pid_t pid = syscall(SYS_gettid); // NOLINT
    cpu_set_t result;

    if (sched_getaffinity(pid, sizeof(cpu_set_t), &result) != 0) { LOG(ERROR); }

    return result;
}

#endif // YAKUSHIMA_LINUX
