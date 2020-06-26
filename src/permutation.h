/**
 * @file permutation.h
 * @brief permutation which expresses the key order by index inside of the border node.
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <iostream>
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
  static constexpr std::size_t cnk_bit_size = 4; // bits
  static constexpr std::size_t pkey_bit_size = 4; // bits, permutation key size.

  permutation() {
    init();
  }

  permutation(std::uint64_t body) {
    set_body(body);
  }

  /**
   * @brief decrement key number.
   */
  void dec_key_num() {
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    // decrement key number
    std::size_t cnk = per_body & cnk_mask;
    --cnk;
    per_body &= ~cnk_mask;
    per_body |= cnk;
    body_.store(per_body, std::memory_order_release);
  }

  void display() {
    std::bitset<64> bs{get_body()};
    cout << " perm : " << bs << endl;
  }

  std::uint64_t get_body() &{
    return body_.load(std::memory_order_acquire);
  }

  uint8_t get_cnk() &{
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    return per_body & cnk_mask;
  }

  [[nodiscard]] std::size_t get_lowest_key_pos() {
    std::uint64_t per = get_body();
    per = per >> cnk_bit_size;
    return per & cnk_mask;
  }

  [[nodiscard]] std::size_t get_index_of_rank(std::size_t rank) {
    std::uint64_t per = get_body();
    per = per >> cnk_bit_size;
    if (rank != 0) {
      per = per >> (pkey_bit_size * rank);
    }
    return per & cnk_mask;
  }

  /**
   * @brief increment key number.
   */
  void inc_key_num() {
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    // increment key number
    std::size_t cnk = per_body & cnk_mask;
#ifndef NDEBUG
    if (cnk >= pow(2, cnk_bit_size) - 1) std::abort();
#endif
    ++cnk;
    per_body &= ~cnk_mask;
    per_body |= cnk;
    body_.store(per_body, std::memory_order_release);
  }

  void init() &{
    body_.store(0, std::memory_order_release);
  }

  /**
   * @pre @a key_slice and @a key_length is array whose size is equal or less than cnk of permutation.
   * If it ignores, it may occur seg-v error.
   * @param key_slice
   * @param key_length
   */
  void rearrange(std::uint64_t *key_slice, std::uint8_t *key_length) {
    std::uint64_t per_body(body_.load(std::memory_order_acquire));
    // get current number of keys
    std::size_t cnk = per_body & cnk_mask;

    // tuple<key_slice, key_length, index>
    constexpr std::size_t key_pos = 2;
    std::vector<std::tuple<base_node::key_slice_type, base_node::key_length_type, std::uint8_t>> vec;
    vec.reserve(cnk);
    for (std::uint8_t i = 0; i < cnk; ++i) {
      vec.emplace_back(key_slice[i], key_length[i], i);
    }
    /**
     * sort based on key_slice and key_length for dictionary order.
     * So <key_slice_type, key_length_type, ...>
     */
    std::sort(vec.begin(), vec.end());

    // rearrange
    std::uint64_t new_body(0);
    for (auto itr = std::crbegin(vec); itr != std::crend(vec); ++itr) {
      new_body |= std::get<key_pos>(*itr);
      new_body <<= pkey_bit_size;
    }
    new_body |= cnk;
    body_.store(new_body, std::memory_order_release);
  }

  void set_body(std::uint64_t new_body) &{
    body_.store(new_body, std::memory_order_release);
  }

  status set_cnk(uint8_t new_cnk) &{
#ifndef NDEBUG
    if (powl(2, cnk_bit_size) <= new_cnk) {
      std::cerr << __FILE__ << " : " << __LINE__ << " : fatal error." << std::endl;
      std::abort();
    }
#endif
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
