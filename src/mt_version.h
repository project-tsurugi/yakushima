/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>

#include "atomic_wrapper.h"
#include "cpu.h"

namespace yakushima {

class node_version64_body {
public:
  [[nodiscard]] bool get_locked() const {
    return locked;
  }

  [[nodiscard]] bool get_inserting() const {
    return inserting;
  }

  [[nodiscard]] bool get_splitting() const {
    return splitting;
  }

  [[nodiscard]] bool get_deleted() const {
    return deleted;
  }

  [[nodiscard]] bool get_root() const {
    return root;
  }

  [[nodiscard]] bool get_leaf() const {
    return leaf;
  }

  [[nodiscard]] std::uint32_t get_vinsert() const {
    return vinsert;
  }

  [[nodiscard]] std::uint64_t get_vsplit() const {
    return vsplit;
  }

  [[nodiscard]] bool get_unused() const {
    return unused;
  }

private:
  bool locked: 1;
  bool inserting: 1;
  bool splitting: 1;
  bool deleted: 1;
  bool root: 1;
  bool leaf: 1;
  std::uint32_t vinsert: 16;
  std::uint64_t vsplit: 41;
  bool unused: 1;
};

class node_version64 {
public:
  void set(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

  /**
   * @pre This function may be interrupted by other writer.
   * So it must use atomic function.
   * @return std::uint64_t
   */
  node_version64_body get_body() const {
    return body_.load(std::memory_order_acquire);
  }

  /**
   * @pre This function may be interrupted by other writer.
   * So it must use atomic function.
   * @return std::uint64_t
   */
  void get_locked() const {
    // todo
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima