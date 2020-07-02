#pragma once

#include <stdint.h>

namespace yakushima {

[[maybe_unused]] static uint64_t rdtsc() {
  uint64_t rax;
  uint64_t rdx;

  asm volatile("cpuid" ::: "rax", "rbx", "rcx", "rdx");
  asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));

  return (rdx << 32) | rax;
}

[[maybe_unused]] static uint64_t rdtscp() {
  uint64_t rax;
  uint64_t rdx;
  uint64_t aux;

  asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux)::);

  return (rdx << 32) | rax;
}

}  // namespace kvs
