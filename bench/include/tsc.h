#pragma once

#include <cstdint>

namespace yakushima {

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

} // namespace yakushima
