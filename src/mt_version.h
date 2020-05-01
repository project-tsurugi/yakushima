/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>

#include "cpu.h"

namespace yakushima {

/**
 * @brief Teh body of node_version64.
 * @details This class is designed to be able to be wrapped by std::atomic,
 * so it can't declare default constructor. Therefore, it should use init function to initialize
 * before using this class object.
 */
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

  void init() {
    locked = false;
    inserting = false;
    splitting = false;
    deleted = false;
    root = false;
    leaf = false;
    vinsert = 0;
    vsplit = 0;
    unused = false;
  }

  void set_locked(bool new_locked) & {
    locked = new_locked;
  }

  void set_inserting(bool new_inserting) & {
    inserting = new_inserting;
  }

  void set_splitting(bool new_splitting) & {
    splitting = new_splitting;
  }

  void set_deleted(bool new_deleted) & {
    deleted = new_deleted;
  }

  void set_root(bool new_root) & {
    root = new_root;
  }

  void set_leaf(bool new_leaf) & {
    leaf = new_leaf;
  }

  void set_vinsert(std::uint32_t new_vinsert) & {
    vinsert = new_vinsert;
  }

  void set_vsplit(std::uint32_t new_vsplit) & {
    vsplit = new_vsplit;
  }

  void set_unused(bool new_unused) & {
    unused = new_unused;
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

/**
 * @brief The class which has atomic<node_version>
 */
class node_version64 {
public:
  /**
   * @details Basically, it should use this default constructor to use init func.
   * Of course, it can use this class without default constructor if it use init function().
   */
  node_version64() {
    init();
  }

  node_version64_body get_body() const {
    return body_.load(std::memory_order_acquire);
  }

  void increment_vinsert() {
    node_version64_body expected(body_.load(std::memory_order_acquire)), desired;
    for (;;) {
      desired = expected;
      desired.set_vinsert(desired.get_vinsert() + 1);
      if (body_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
    }
  }

  /**
   * @pre This function is called by only single thread.
   */
  void init() {
    node_version64_body verbody;
    verbody.init();
    body_.store(verbody, std::memory_order_release);
  }

  void set(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima