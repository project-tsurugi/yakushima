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
 * @brief Teh body of node_version64.
 * @details This class is designed to be able to be wrapped by std::atomic,
 * so it can't declare default constructor. Therefore, it should use init function to initialize
 * before using this class object.
 */
class node_version64_body {
public:
  bool operator==(const node_version64_body& rhs) const {
    return locked == rhs.get_locked()
           && inserting == rhs.get_inserting()
           && splitting == rhs.get_splitting()
           && deleted == rhs.get_deleted()
           && root == rhs.get_root()
           && leaf == rhs.get_leaf()
           && vinsert == rhs.get_vinsert()
           && vsplit == rhs.get_vsplit()
           && unused == rhs.get_unused();
  }

  bool operator!=(const node_version64_body& rhs) const {
    return !(*this == rhs);
  }

  [[nodiscard]] bool get_deleted() const {
    return deleted;
  }

  [[nodiscard]] bool get_inserting() const {
    return inserting;
  }

  [[nodiscard]] bool get_leaf() const {
    return leaf;
  }

  [[nodiscard]] bool get_locked() const {
    return locked;
  }

  [[nodiscard]] bool get_root() const {
    return root;
  }

  [[nodiscard]] bool get_splitting() const {
    return splitting;
  }

  [[nodiscard]] bool get_unused() const {
    return unused;
  }

  [[nodiscard]] std::uint32_t get_vinsert() const {
    return vinsert;
  }

  [[nodiscard]] std::uint64_t get_vsplit() const {
    return vsplit;
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

  void set_deleted(bool new_deleted) & {
    deleted = new_deleted;
  }

  void set_inserting(bool new_inserting) & {
    inserting = new_inserting;
  }

  void set_leaf(bool new_leaf) & {
    leaf = new_leaf;
  }

  void set_locked(bool new_locked) & {
    locked = new_locked;
  }

  void set_root(bool new_root) & {
    root = new_root;
  }

  void set_splitting(bool new_splitting) & {
    splitting = new_splitting;
  }

  void set_unused(bool new_unused) & {
    unused = new_unused;
  }

  void set_vinsert(std::uint32_t new_vinsert) & {
    vinsert = new_vinsert;
  }

  void set_vsplit(std::uint32_t new_vsplit) & {
    vsplit = new_vsplit;
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

  node_version64(node_version64 const &rv) {
    body_.store(rv.get_body(), std::memory_order_release);
  }

  /**
   * @details This is atomic increment.
   * If you use "setter(getter + 1)", that is not atomic increment.
   */
  void atomic_increment_vinsert() {
    node_version64_body expected(body_.load(std::memory_order_acquire)), desired;
    for (;;) {
      desired = expected;
      desired.set_vinsert(desired.get_vinsert() + 1);
      if (body_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
    }
  }

  /**
   * @details This function locks atomically.
   * @return void
   */
  void lock() {
    node_version64_body expected(body_.load(std::memory_order_acquire)), desired;
    for (;;) {
      if (expected.get_locked()) {
        _mm_pause();
        expected = body_.load(std::memory_order_acquire);
        continue;
      }
      desired = expected;
      desired.set_locked(true);
      if (body_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
    }
  }

  /**
   * @details This function unlocks atomically.
   * @pre The caller already succeeded acquiring lock.
   */
  void unlock() & {
    node_version64_body desired(body_.load(std::memory_order_acquire));
    if (desired.get_inserting()) {
      desired.set_vinsert(desired.get_vinsert() + 1);
      desired.set_inserting(false);
    }
    if (desired.get_splitting()) {
      desired.set_vsplit(desired.get_vsplit() + 1);
      desired.set_splitting(false);
    }
    desired.set_locked(false);
    body_.store(desired, std::memory_order_release);
  }

  [[nodiscard]] node_version64_body get_body() const {
    return body_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool get_deleted() const {
    return get_body().get_deleted();
  }

  [[nodiscard]] bool get_inserting() const {
    return get_body().get_inserting();
  }

  [[nodiscard]] bool get_leaf() const {
    return get_body().get_leaf();
  }

  [[nodiscard]] bool get_locked() const {
    return get_body().get_locked();
  }

  [[nodiscard]] bool get_root() const {
    return get_body().get_root();
  }

  [[nodiscard]] bool get_splitting() const {
    return get_body().get_splitting();
  }

  [[nodiscard]] node_version64_body get_stable_version() const {
    for (;;) {
      node_version64_body sv = get_body();
      if (sv.get_inserting() == false && sv.get_splitting() == false)
        return sv;
    }
  }
  [[nodiscard]] bool get_unused() const {
    return get_body().get_unused();
  }

  [[nodiscard]] bool get_vinsert() const {
    return get_body().get_vinsert();
  }

  [[nodiscard]] bool get_vsplit() const {
    return get_body().get_vsplit();
  }

  /**
   * @pre This function is called by only single thread.
   */
  void init() {
    node_version64_body verbody;
    verbody.init();
    body_.store(verbody, std::memory_order_release);
  }

  void set_body(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

  void set_deleted(bool new_deleted) & {
    node_version64_body new_body = get_body();
    new_body.set_deleted(new_deleted);
    set_body(new_body);
  }

  void set_inserting(bool new_inserting) & {
    node_version64_body new_body = get_body();
    new_body.set_inserting(new_inserting);
    set_body(new_body);
  }

  void set_leaf(bool new_leaf) & {
    node_version64_body new_body = get_body();
    new_body.set_leaf(new_leaf);
    set_body(new_body);
  }

  void set_locked(bool new_locked) & {
    node_version64_body new_body = get_body();
    new_body.set_locked(new_locked);
    set_body(new_body);
  }

  void set_root(bool new_root) & {
    node_version64_body new_body = get_body();
    new_body.set_root(new_root);
    set_body(new_body);
  }

  void set_splitting(bool new_splitting) & {
    node_version64_body new_body = get_body();
    new_body.set_splitting(new_splitting);
    set_body(new_body);
  }

  void set_unused(bool new_unused) & {
    node_version64_body new_body = get_body();
    new_body.set_unused(new_unused);
    set_body(new_body);
  }

  void set_vinsert(bool new_vinsert) & {
    node_version64_body new_body = get_body();
    new_body.set_vinsert(new_vinsert);
    set_body(new_body);
  }

  void set_vsplit(bool new_vsplit) & {
    node_version64_body new_body = get_body();
    new_body.set_vsplit(new_vsplit);
    set_body(new_body);
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima