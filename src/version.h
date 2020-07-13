/**
 * @file version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>
#include <xmmintrin.h>

#include "atomic_wrapper.h"

namespace yakushima {

/**
 * @brief Teh body of node_version64.
 * @details This class is designed to be able to be wrapped by std::atomic,
 * so it can't declare default constructor. Therefore, it should use init function to initialize
 * before using this class object.
 */
class node_version64_body {
public:
  using vdelete_type = std::uint16_t;
  using vinsert_type = std::uint16_t;
  using vsplit_type = std::uint16_t;

  node_version64_body() = default;

  node_version64_body(const node_version64_body &) = default;

  node_version64_body(node_version64_body &&) = default;

  node_version64_body &operator=(const node_version64_body &) = default;

  node_version64_body &operator=(node_version64_body &&) = default;

  ~node_version64_body() = default;

  bool operator==(const node_version64_body &rhs) const {
    return locked == rhs.get_locked()
           && deleting_node == rhs.get_deleting_node()
           && inserting == rhs.get_inserting()
           && splitting == rhs.get_splitting()
           && deleted == rhs.get_deleted()
           && root == rhs.get_root()
           && border == rhs.get_border()
           && unused == rhs.get_unused()
           && vdelete == rhs.get_vdelete()
           && vinsert == rhs.get_vinsert()
           && vsplit == rhs.get_vsplit();
  }

  /**
   * @details display function for analysis and debug.
   */
  void display() const {
    std::cout << "node_version64_body::display" << std::endl;
    std::cout << "locked : " << get_locked() << std::endl;
    std::cout << "deleting : " << get_deleting_node() << std::endl;
    std::cout << "inserting : " << get_inserting() << std::endl;
    std::cout << "splitting : " << get_splitting() << std::endl;
    std::cout << "deleted : " << get_deleted() << std::endl;
    std::cout << "root : " << get_root() << std::endl;
    std::cout << "border : " << get_border() << std::endl;
    std::cout << "unused : " << get_unused() << std::endl;
    std::cout << "vdelete : " << get_vdelete() << std::endl;
    std::cout << "vinsert : " << get_vinsert() << std::endl;
    std::cout << "vsplit: " << get_vsplit() << std::endl;
  }

  bool operator!=(const node_version64_body &rhs) const {
    return !(*this == rhs);
  }

  [[nodiscard]] bool get_border() const {
    return border;
  }

  [[nodiscard]] bool get_deleted() const {
    return deleted;
  }

  [[nodiscard]] bool get_deleting_node() const {
    return deleting_node;
  }

  [[nodiscard]] bool get_inserting() const {
    return inserting;
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

  [[nodiscard]] vdelete_type get_vdelete() const {
    return vdelete;
  }

  [[nodiscard]] vinsert_type get_vinsert() const {
    return vinsert;
  }

  [[nodiscard]] vsplit_type get_vsplit() const {
    return vsplit;
  }

  void inc_vdelete() {
    ++vdelete;
  }

  void inc_vinsert() {
    ++vinsert;
  }

  void inc_vsplit() {
    ++vsplit;
  }

  void init() {
    locked = false;
    deleting_node = false;
    inserting = false;
    splitting = false;
    deleted = false;
    root = false;
    border = false;
    unused = false;
    vdelete = 0;
    vinsert = 0;
    vsplit = 0;
  }

  void make_stable_version_forcibly() {
    set_deleting_node(false);
    set_inserting(false);
    set_locked(false);
    set_splitting(false);
  }

  void set_border(bool new_border) &{
    border = new_border;
  }

  void set_deleted(bool new_deleted) &{
    deleted = new_deleted;
  }

  void set_deleting_node(bool tf) &{
    deleting_node = tf;
  }

  void set_inserting(bool new_inserting) &{
    inserting = new_inserting;
  }

  void set_locked(bool new_locked) &{
    locked = new_locked;
  }

  void set_root(bool new_root) &{
    root = new_root;
  }

  void set_splitting(bool new_splitting) &{
    splitting = new_splitting;
  }

  void set_unused(bool new_unused) &{
    unused = new_unused;
  }

  void set_vdelete(vdelete_type new_vdelete) {
    vdelete = new_vdelete;
  }

  void set_vinsert(vinsert_type new_vinsert) &{
    vinsert = new_vinsert;
  }

  void set_vsplit(vsplit_type new_vsplit) &{
    vsplit = new_vsplit;
  }

private:
  /**
   * These details is based on original paper Fig. 3.
   */
  /**
   * @details It is claimed by update or insert.
   */
  bool locked: 1;
  /**
   * @details It shows that this nodes will be deleted.
   * The function find_lowest_key takes the value from the node when this flag is up. Read. When we raise this flag,
   * we guarantee that no problems will occur with it.
   */
  bool deleting_node: 1;
  /**
   * @details It is a dirty bit set during inserting.
   */
  bool inserting: 1;
  /**
   * @details It is a dirty bit set during splitting.
   * If this flag is set, vsplit is incremented when the lock is unlocked.
   * The function find_lowest_key takes the value from the node when this flag is up. Read. When we raise this flag,
   * we guarantee that no problems will occur with it.
   */
  bool splitting: 1;
  /**
   * @details It is a delete bit set after delete.
   */
  bool deleted: 1;
  /**
   * @details It tells whether the node is the root of some B+-tree.
   */
  bool root: 1;
  /**
   * @details It tells whether the node is interior or border.
   */
  bool border: 1;
  /**
   * @details It allows more efficient operations on the version number.
   * tanabe : Is variables's details is nothing in original paper?
   */
  bool unused: 1;
  /**
   * @attention tanabe : In the original paper, the interior node does not have a delete count field.
   * On the other hand, the border node has this (nremoved) but no details in original paper.
   * Since there is a @a deleted field in the version, you can check whether the node you are checking has been deleted.
   * However, you do not know that the position has been moved.
   * Maybe you took the node from the wrong position.
   * The original paper cannot detect it.
   * Therefore, add this vdelete field.
   * @details It is a counter incremented after delete.
   */
  vdelete_type vdelete;
  /**
   * @details It is a counter incremented after inserting.
   */
  vinsert_type vinsert;
  /**
   * @details It is a counter incremented after splitting.
   */
  vsplit_type vsplit;
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

  /**
   * @details This is atomic increment.
   * If you use "setter(getter + 1)", that is not atomic increment.
   */
  void atomic_inc_vinsert() {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.inc_vinsert();
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_inc_vdelete() {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.inc_vdelete();
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_set_border(bool tf) {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_border(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_set_deleted(bool tf) {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_deleted(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_set_deleting_node(bool tf) &{
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_deleting_node(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_set_inserting(bool tf) &{
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_inserting(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
}
    }
  }

  void atomic_set_locked(bool tf) {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_locked(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
      }
    }
  }

  void atomic_set_root(bool tf) {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_root(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
      }
    }
  }

  void atomic_set_splitting(bool tf) {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      desired.set_splitting(tf);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
      }
    }
  }

  /**
   * @details display function for analysis and debug.
   */
  void display() {
    get_body().display();
  }

  /**
   * @details This function locks atomically.
   * @return void
   */
  void lock() {
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      if (expected.get_locked()) {
        _mm_pause();
        expected = get_body();
        continue;
      }
      desired = expected;
      desired.set_locked(true);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        break;
      }
    }
  }

  [[nodiscard]] node_version64_body get_body() {
    return body_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool get_border() {
    return get_body().get_border();
  }

  [[nodiscard]] bool get_deleted() {
    return get_body().get_deleted();
  }

  [[nodiscard]] bool get_deleting() {
    return get_body().get_deleting_node();
  }

  [[nodiscard]] bool get_inserting() {
    return get_body().get_inserting();
  }

  [[nodiscard]] bool get_locked() {
    return get_body().get_locked();
  }

  [[nodiscard]] bool get_root() {
    return get_body().get_root();
  }

  [[nodiscard]] bool get_splitting() {
    return get_body().get_splitting();
  }

  [[nodiscard]] node_version64_body get_stable_version() {
    for (;;) {
      node_version64_body sv = get_body();
      /**
       * In the original paper, lock is not checked.
       * However, if the lock is acquired, the member of that node can be changed.
       * Even if the locked version is immutable, the members read at that time may be broken.
       * Therefore, you have to check the lock.
       */
      if (!sv.get_deleting_node() && !sv.get_inserting() && !sv.get_locked() && !sv.get_splitting()) {
        return sv;
      }
      _mm_pause();
    }
  }

  [[nodiscard]] bool get_unused() {
    return get_body().get_unused();
  }

  [[nodiscard]] node_version64_body::vdelete_type get_vdelete() {
    return get_body().get_vdelete();
  }

  [[nodiscard]] node_version64_body::vinsert_type get_vinsert() {
    return get_body().get_vinsert();
  }

  [[nodiscard]] node_version64_body::vsplit_type get_vsplit() {
    return get_body().get_vsplit();
  }

  /**
   * @pre This function is called by only single thread.
   */
  void init() {
    set_body(node_version64_body());
  }

  void make_stable_version_forcibly() {
    atomic_set_deleting_node(false);
    atomic_set_inserting(false);
    atomic_set_locked(false);
    atomic_set_splitting(false);
  }

  void set_body(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

  /**
   * @details This function unlocks @a atomically.
   * @pre The caller already succeeded acquiring lock.
   */
  void unlock() &{
    node_version64_body expected(get_body());
    node_version64_body desired{};
    for (;;) {
      desired = expected;
      if (desired.get_inserting()) {
        desired.inc_vinsert();
        desired.set_inserting(false);
      }
      if (desired.get_splitting()) {
        desired.inc_vsplit();
        desired.set_splitting(false);
      }
      desired.set_deleting_node(false);
      desired.set_locked(false);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) break;
    }
  }

  static void unlock(std::vector<node_version64 *> &lock_list) {
    for (auto &&l : lock_list) {
      l->unlock();
    }
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima