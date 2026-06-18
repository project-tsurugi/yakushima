#pragma once

#include <cstdint>

namespace yakushima {

#if defined(__x86_64__)

[[maybe_unused]] static uint64_t rdtsc() {
    uint64_t rax{};
    uint64_t rdx{};

    asm volatile("cpuid" :: // NOLINT
                 : "rax", "rbx", "rcx", "rdx");
    asm volatile("rdtsc" // NOLINT
                 : "=a"(rax), "=d"(rdx));

    return rdx << 32U | rax;
}

[[maybe_unused]] static uint64_t rdtscp() {
    uint64_t rax{};
    uint64_t rdx{};
    uint64_t aux{};

    asm volatile("rdtscp" // NOLINT
                 : "=a"(rax), "=d"(rdx), "=c"(aux)::);

    return rdx << 32U | rax;
}

#elif defined(__aarch64__)

// Use the AArch64 virtual system counter as a monotonic timestamp.
[[maybe_unused]] static uint64_t rdtsc() {
    uint64_t v{};
    asm volatile("mrs %0, cntvct_el0" : "=r"(v)); // NOLINT
    return v;
}

// isb ensures the counter is read after preceding instructions complete.
[[maybe_unused]] static uint64_t rdtscp() {
    uint64_t v{};
    asm volatile("isb; mrs %0, cntvct_el0" : "=r"(v)); // NOLINT
    return v;
}

#endif

} // namespace yakushima
