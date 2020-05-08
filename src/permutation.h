/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <array>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <xmmintrin.h>

#include "base_node.h"
#include "scheme.h"

namespace yakushima {

class permutation {
public:
  static constexpr std::size_t cnk_mask = 0b1111;
  static constexpr std::size_t cnk_size = 4; // bits

  permutation() {
    init();
  }

  std::uint64_t get_body() &{
    return body_.load(std::memory_order_acquire);
  }

  uint8_t get_cnk() &{
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    return per_body & cnk_mask;
  }

  void init() &{
    body_.store(0, std::memory_order_release);
  }

  void set_body(std::uint64_t new_body) &{
    body_.store(new_body, std::memory_order_release);
  }

  status set_cnk(uint8_t new_cnk) &{
    if (powl(2, cnk_size) <= new_cnk) return status::WARN_BAD_USAGE;
    std::uint64_t body = body_.load(std::memory_order_acquire);
    body &= ~cnk_mask;
    body |= new_cnk;
    body_.store(body, std::memory_order_release);
    return status::OK;
  }

private:
  std::atomic<uint64_t> body_;
};

} // namespace yakushima
