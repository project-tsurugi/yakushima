/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <xmmintrin.h>

namespace yakushima {

/**
 * @brief Masstree Permutation
 * @details The permutation compactly represents the correct key order
 * plus the current number of keys.
 */
class permutation_body {
public:
  void init() {
    cko_ = 0;
    cnk_ = 0;
  }

  uint8_t get_cnk() & {
    return cnk_;
  }

  void set_cnk(uint8_t new_cnk) & {
    cnk_ = new_cnk;
  }
private:
  /**
   * @brief Correct key order
   * @details 1 element is 4 bits, so there are 15 elements.
   */
  std::uint64_t cko_ : 60;
  std::uint8_t cnk_ : 4;
};

class permutation {
public:
  permutation() {
    init();
  }

  permutation_body get_body() & {
    return body_.load(std::memory_order_acquire);
  }

  uint8_t get_cnk() & {
    permutation_body perbody(body_.load(std::memory_order_acquire));
    return perbody.get_cnk();
  }

  void init() & {
    permutation_body perbody;
    perbody.init();
    body_.store(perbody, std::memory_order_release);
  }

  void set_body(permutation_body new_body) & {
    body_.store(new_body, std::memory_order_release);
  }
private:
  std::atomic<permutation_body> body_;
};

} // namespace yakushima