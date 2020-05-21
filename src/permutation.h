/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <tuple>
#include <vector>

#include <xmmintrin.h>

#include "base_node.h"
#include "scheme.h"

namespace yakushima {

class permutation {
public:
  /**
   * cnk ... current number of keys.
   */
  static constexpr std::size_t cnk_mask = 0b1111;
  static constexpr std::size_t cnk_size = 4; // bits
  static constexpr std::size_t pkey_size = 4; // bits, permutation key size.

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

  /**
   * @brief increment key number.
   */
  void inc_key_num() {
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    // increment key number
    std::size_t cnk = per_body & cnk_mask;
    if (cnk >= pow(2, cnk_size) -1) std::abort();
    ++cnk;
    per_body &= ~cnk_mask;
    per_body |= cnk;
    body_.store(per_body, std::memory_order_release);
  }

  void init() &{
    body_.store(0, std::memory_order_release);
  }

  void rearrange(std::uint64_t *key_slice, std::uint8_t *key_length) {
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    // get current number of keys
    std::size_t cnk = per_body & cnk_mask;

    // tuple<key_slice, key_length, index>
    [[maybe_unused]] constexpr std::size_t key_slice_index = 0;
    [[maybe_unused]] constexpr std::size_t key_length_index = 1;
    constexpr std::size_t key_pos = 2;
    std::vector<std::tuple<base_node::key_slice_type, base_node::key_length_type, std::uint8_t>> vec;
    vec.reserve(cnk);
    for (std::uint8_t i = 0; i < cnk; ++i) {
      vec.emplace_back(key_slice[i], key_length[i], i);
    }
    std::sort(vec.begin(), vec.end());

    // rearrange
    std::uint64_t new_body(0);
    for (auto itr = std::crbegin(vec); itr != std::crend(vec); ++itr) {
      new_body |= std::get<key_pos>(*itr);
      new_body <<= pkey_size;
    }
    new_body |= cnk;
    body_.store(new_body, std::memory_order_release);
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
