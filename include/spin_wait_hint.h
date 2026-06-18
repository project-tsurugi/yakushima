/**
 * @file cpu_pause.h
 * @brief Portable spin-loop hint.
 */

#pragma once

#if defined(__x86_64__)
#include <emmintrin.h>
#endif

namespace yakushima {

/**
 * @brief Hint to the CPU that this is a spin-wait loop.
 */
static inline void spin_wait_hint() noexcept { // NOLINT
#if defined(__x86_64__)
    _mm_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}

} // namespace yakushima
