/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <xmmintrin.h>

#include "atomic_wrapper.h"

using std::cout, std::endl;

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
  void display() {
    cout << "node_version64_body::display" << endl;
    cout << "locked : " << get_locked() << endl;
    cout << "inserting : " << get_inserting() << endl;
    cout << "splitting : " << get_splitting() << endl;
    cout << "deleted : " << get_deleted() << endl;
    cout << "root : " << get_root() << endl;
    cout << "border : " << get_border() << endl;
    cout << "unused : " << get_unused() << endl;
    cout << "vdelete : " << get_vdelete() << endl;
    cout << "vinsert : " << get_vinsert() << endl;
    cout << "vsplit: " << get_vsplit() << endl;
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

  void init() {
    locked = false;
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

  void set_border(bool new_border) &{
    border = new_border;
  }

  void set_deleted(bool new_deleted) &{
    deleted = new_deleted;
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
   * @details It is a dirty bit set during inserting.
   */
  bool inserting: 1;
  /**
   * @details It is a dirty bit set during splitting.
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
  void atomic_increment_vinsert() {
    node_version64_body expected(get_body()), desired;
    for (;;) {
      desired = expected;
      desired.set_vinsert(desired.get_vinsert() + 1);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
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
    node_version64_body expected(get_body()), desired;
    for (;;) {
      if (expected.get_locked()) {
        _mm_pause();
        expected = get_body();
        continue;
      }
      desired = expected;
      desired.set_locked(true);
      if (body_.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
        break;
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
      if (sv.get_inserting() == false && sv.get_locked() == false && sv.get_splitting() == false)
        return sv;
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

  void set_body(node_version64_body newv) &{
    body_.store(newv, std::memory_order_release);
  }

  void set_border(bool new_border) &{
    node_version64_body new_body = get_body();
    new_body.set_border(new_border);
    set_body(new_body);
  }

  void set_deleted(bool new_deleted) &{
    node_version64_body new_body = get_body();
    new_body.set_deleted(new_deleted);
    set_body(new_body);
  }

  void set_inserting(bool new_inserting) &{
    node_version64_body new_body = get_body();
    new_body.set_inserting(new_inserting);
    set_body(new_body);
  }

  void set_locked(bool new_locked) &{
    node_version64_body new_body = get_body();
    new_body.set_locked(new_locked);
    set_body(new_body);
  }

  void set_root(bool new_root) &{
    node_version64_body new_body = get_body();
    new_body.set_root(new_root);
    set_body(new_body);
  }

  void set_splitting(bool new_splitting) &{
    node_version64_body new_body = get_body();
    new_body.set_splitting(new_splitting);
    set_body(new_body);
  }

  void set_unused(bool new_unused) &{
    node_version64_body new_body = get_body();
    new_body.set_unused(new_unused);
    set_body(new_body);
  }

  void set_vdelete(node_version64_body::vdelete_type new_vdelete) &{
    node_version64_body new_body = get_body();
    new_body.set_vdelete(new_vdelete);
    set_body(new_body);
  }

  void set_vinsert(node_version64_body::vinsert_type new_vinsert) &{
    node_version64_body new_body = get_body();
    new_body.set_vinsert(new_vinsert);
    set_body(new_body);
  }

  void set_vsplit(node_version64_body::vsplit_type new_vsplit) &{
    node_version64_body new_body = get_body();
    new_body.set_vsplit(new_vsplit);
    set_body(new_body);
  }

  /**
   * @details This function unlocks atomically.
   * @pre The caller already succeeded acquiring lock.
   */
  void unlock() &{
    node_version64_body desired(get_body());
    if (desired.get_inserting()) {
      desired.set_vinsert(desired.get_vinsert() + 1);
      desired.set_inserting(false);
    }
    if (desired.get_splitting()) {
      desired.set_vsplit(desired.get_vsplit() + 1);
      desired.set_splitting(false);
    }
    desired.set_locked(false);
    set_body(desired);
  }

private:
  std::atomic<node_version64_body> body_;
};

} // namespace yakushima