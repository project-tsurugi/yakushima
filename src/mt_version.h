/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cstdint>

namespace yakushima {

#if 0
class node_version32 {
public:
  enum class node_version_params32 : std::uint32_t {
    lock_bit = (1U << 0),
    inserting_bit = (1U << 1),
    splitting_bit = (1U << 2),
    dirty_mask = inserting_bit | splitting_bit, // use for stableversion
    unlock_mask = lock_bit | inserting_bit | splitting_bit, // use for unlock
    deleted_bit = (1U << 3),
    root_bit = (1U << 4),
    leaf_bit = (1U << 5),
    unused_bit = (1U << 6),
    vinsert_lowbit = (1U << 7),
    vinsert_topbit = (1U << 14), // vinsert -> 8 bits
    vsplit_lowbit = (1U << 15),
    vsplit_topbit = (1U << 31) // vsplit -> 17 bits
  };

  node_version32() : version_(0) {}

private:
  std::atomic<uint32_t> version_;
};
#endif

class node_version64 {
public:
  enum class node_version_params64 : std::uint64_t {
    lock_bit = (1ull << 0),
    inserting_bit = (1ull << 1),
    splitting_bit = (1ull << 2),
    dirty_mask = inserting_bit | splitting_bit, // use for stableversion
    unlock_mask = lock_bit | inserting_bit | splitting_bit, // use for unlock
    deleted_bit = (1ull << 3),
    root_bit = (1ull << 4),
    leaf_bit = (1ull << 5),
    unused_bit = (1ull << 6),
    vinsert_lowbit = (1ull << 7),
    vinsert_topbit = (1ull << 22), // vinsert -> 16 bits
    vsplit_lowbit = (1ull << 23),
    vsplit_topbit = (1ull << 63) // vsplit -> 41 bits
  };

  node_version64() : version_(0) {}

  node_version64(std::uint64_t ver) : version_(ver) {}

  [[nodiscard]] static bool check_inserting_bit(std::uint64_t v) {
    return (v & static_cast<std::uint64_t>(node_version_params64::inserting_bit)) ==
           static_cast<std::uint64_t>(node_version_params64::inserting_bit);
  }

  [[nodiscard]] static bool check_lock_bit(std::uint64_t v) {
    return (v & static_cast<std::uint64_t>(node_version_params64::lock_bit)) ==
           static_cast<std::uint64_t>(node_version_params64::lock_bit);
  }

  [[nodiscard]] bool check_lock_bit() &{
    return (version_.load(std::memory_order_acquire) & static_cast<std::uint64_t>(node_version_params64::lock_bit)) ==
           static_cast<std::uint64_t>(node_version_params64::lock_bit);
  }

  [[nodiscard]] static bool check_splitting_bit(std::uint64_t v) {
    return (v & static_cast<std::uint64_t>(node_version_params64::splitting_bit)) ==
           static_cast<std::uint64_t>(node_version_params64::splitting_bit);
  }

  [[nodiscard]] uint64_t get_version() const &{
    return version_.load(std::memory_order_acquire);
  }

  [[nodiscard]] uint64_t get_stable_version() const &{
    for (;;) {
      uint64_t ver(version_.load(std::memory_order_acquire));
      if (check_lock_bit(ver) == false &&
          check_splitting_bit(ver) == false)
        return ver;
      else continue;
    }
  }

  /**
   * @brief lock
   * tanabe : この関数は並行に呼び出されたらまずい？そこまで実装してみないと未来のことは良くわからない．
   */
  void lock() &{
    uint64_t expected(version_.load(std::memory_order_acquire)), desired;
    for (;;) {
      if (check_lock_bit(expected)) {
        expected = version_.load(std::memory_order_acquire);
        continue;
      }
      desired = expected;
      set_lock_bit(desired);
      if (version_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
    }
  }

  static void set_lock_bit(std::uint64_t &v) {
    v = v | static_cast<std::uint64_t>(node_version_params64::lock_bit);
  }

  /**
   * @brief set_vinsert
   * @pre This function can't be called concurrently.
   * @param rvinsert
   * @return void
   */
  void set_vinsert(std::uint64_t rvinsert) & {
    std::bitset<64> bs(rvinsert);
    // vinsert is 16 bits, so it delete more than 16 bits.
    bs <<= (64 - 16);
    // it repair bits position correctly.
    bs >>= (64 - (static_cast<uint64_t>(node_version_params64::vinsert_topbit) / 2) - 1);
    version_.store(version_.load(std::memory_order_acquire) ^ bs.to_ullong(), std::memory_order_release);
  }

  void unlock() & {
    uint64_t new_ver(version_.load(std::memory_order_acquire));
#if 0
    // todo
    if (check_inserting_bit(new_ver))
      set_vinsert(get_vinsert() + 1);
    if (check_splitting_bit((new_ver)))
      set_vsplit(get_vsplit() + 1);
#endif
    unset_inserting_bit(new_ver);
    unset_lock_bit(new_ver);
    unset_splitting_bit(new_ver);
    version_.store(new_ver, std::memory_order_release);
  }

  static void unset_inserting_bit(uint64_t& v) {
    v = v ^ static_cast<std::uint64_t>(node_version_params64::inserting_bit);
  }

  static void unset_lock_bit(uint64_t& v) {
    v = v ^ static_cast<std::uint64_t>(node_version_params64::lock_bit);
  }

  static void unset_splitting_bit(uint64_t& v) {
    v = v ^ static_cast<std::uint64_t>(node_version_params64::splitting_bit);
  }

private:
  std::atomic<uint64_t> version_;
};

} // namespace yakushima