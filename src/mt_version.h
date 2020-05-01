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

class node_version64 {
public:
  void set(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

  node_version64_body get_body() const {
    return body_.load(std::memory_order_acquire);
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima