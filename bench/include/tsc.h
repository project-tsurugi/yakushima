#pragma once

#include <cstdint>

namespace yakushima {

[[maybe_unused]] static uint64_t rdtsc() {
  uint64_t rax{};
  uint64_t rdx{};

  asm volatile("cpuid" ::: "rax", "rbx", "rcx", "rdx"); // NOLINT
  asm volatile("rdtsc" : "=a"(rax), "=d"(rdx)); // NOLINT

  return rdx << 32U | rax;
}

[[maybe_unused]] static uint64_t rdtscp() {
  uint64_t rax{};
  uint64_t rdx{};
  uint64_t aux{};

  asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::); // NOLINT

  return rdx << 32U | rax;
}

}  // namespace kvs
