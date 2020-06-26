/**
 * @file base_node.h
 */

#pragma once

#include <cstring>

#include "atomic_wrapper.h"
#include "cpu.h"
#include "scheme.h"
#include "version.h"

namespace yakushima {

/**
 * forward declaration to use friend
 */
class masstree_kvs;

class base_node {
public:
  static constexpr std::size_t key_slice_length = 15;
  using key_slice_type = std::uint64_t;
  /**
   * key_length_type is used at permutation.h, border_node.h.
   * To avoid circular reference at there, declare here.
   */
  using key_length_type = std::uint8_t;
  /**
   * These are mainly used at link_or_value.h
   * To avoid circular reference at there, declare here.
   */
  using value_length_type = std::size_t;
  using value_align_type = std::size_t;


  base_node() = default;

  ~base_node() = default;

  void atomic_set_version_root(bool tf) {
    version_.atomic_set_root(tf);
  }

  /**
   * A virtual function is defined because It wants to distinguish the children class of the contents
   * by using polymorphism.
   * So this function is pure virtual function.
   */
  virtual status destroy() = 0;

  /**
   * @details display function for analysis and debug.
   */
  virtual void display() = 0;

  void display_base() {
    cout << "base_node::display_base" << endl;
    version_.display();
    cout << "parent_ : " << get_parent() << endl;
    for (std::size_t i = 0; i < key_slice_length; ++i) {
      cout << "key_slice_[" << i << "] : " << std::to_string(get_key_slice_at(i)) << endl;
      cout << "key_length_[" << i << "] : " << std::to_string(get_key_length_at(i)) << endl;
    }
  }

  [[nodiscard]] key_length_type *get_key_length() {
    return key_length_;
  }

  [[nodiscard]] key_length_type get_key_length_at(std::size_t index) {
    return loadAcquireN(key_length_[index]);
  }

  [[nodiscard]] key_slice_type *get_key_slice() {
    return key_slice_;
  }

  [[nodiscard]] key_slice_type get_key_slice_at(std::size_t index) {
    if (index > static_cast<std::size_t>(key_slice_length)) {
      std::abort();
    }
    return loadAcquireN(key_slice_[index]);
  }

  [[nodiscard]] bool get_lock() &{
    return version_.get_locked();
  }

  static base_node *get_root() {
    return root_.load(std::memory_order_acquire);
  }

  [[nodiscard]] node_version64_body get_stable_version() &{
    return version_.get_stable_version();
  }

  [[nodiscard]] base_node *get_parent() &{
    return loadAcquireN(parent_);
  }

  [[nodiscard]] node_version64_body get_version() &{
    return version_.get_body();
  }

  [[nodiscard]] node_version64 *get_version_ptr() {
    return &version_;
  }

  [[nodiscard]] bool get_version_border() {
    return version_.get_border();
  }

  [[nodiscard]] bool get_version_deleted() {
    return version_.get_deleted();
  }

  [[nodiscard]] bool get_version_root() {
    return version_.get_root();
  }

  [[nodiscard]] node_version64_body::vdelete_type get_version_vdelete() {
    return version_.get_vdelete();
  }

  [[nodiscard]] node_version64_body::vinsert_type get_version_vinsert() {
    return version_.get_vinsert();
  }

  [[nodiscard]] node_version64_body::vsplit_type get_version_vsplit() {
    return version_.get_vsplit();
  }

  void init_base() {
    version_.init();
    set_parent(nullptr);
    init_base_member_range(0, key_slice_length - 1);
  }

  /**
   * @details init at @a pos as position.
   * @param[in] pos This is a position (index) to be initialized.
   */
  void init_base(std::size_t pos) {
    set_key(pos, 0, 0);
  }

  void init_base_member_range(std::size_t start, std::size_t end) {
    for (std::size_t i = start; i <= end; ++i) {
      set_key(i, 0, 0);
    }
  }

  /**
   * @brief It locks this node.
   * @pre It didn't lock by myself.
   * @return void
   */
  void lock() &{
    version_.lock();
  }

  /**
   * @pre This function is called by split.
   */
  base_node *lock_parent() &{
    base_node *p = get_parent();
    for (;;) {
      if (p == nullptr) return nullptr;
      p->lock();
      base_node *check = get_parent();
      if (p == check) {
        return p;
      } else {
        p->version_unlock();
        p = check;
      }
    }
  }

  void move_key_to_base_range(base_node *right, std::size_t start) {
    for (auto i = start; i < key_slice_length; ++i) {
      right->set_key(i - start, get_key_slice_at(i), get_key_length_at(i));
      set_key(i, 0, 0);
    }
  }

  void set_key(std::size_t index, key_slice_type key_slice, key_length_type key_length) {
    set_key_slice_at(index, key_slice);
    set_key_length_at(index, key_length);
  }

  void set_key_length_at(std::size_t index, key_length_type length) {
    storeReleaseN(key_length_[index], length);
  }

  void set_key_slice_at(std::size_t index, key_slice_type key_slice) {
    if (index >= key_slice_length) std::abort();
    storeReleaseN(key_slice_[index], key_slice);
  }

  void set_parent(base_node *new_parent) {
    storeReleaseN(parent_, new_parent);
  }

  static void set_root(base_node *nr) {
    root_.store(nr, std::memory_order_release);
  }

  void set_version(node_version64_body nv) { // this function is used.
    version_.set_body(nv);
  }

  void set_version_border(bool tf) {
    version_.atomic_set_border(tf);
  }

  void set_version_deleted(bool tf) {
    version_.atomic_set_deleted(tf);
  }

  void set_version_deleting_node(bool tf) {
    version_.atomic_set_deleting_node(tf);
  }

  void set_version_inserting(bool tf) {
    version_.atomic_set_inserting(tf);
  }

  void set_version_root(bool tf) {
    version_.atomic_set_root(tf);
  }

  void set_version_splitting(bool tf) {
    version_.atomic_set_splitting(tf);
  }

  void shift_left_base_member(std::size_t start_pos, std::size_t shift_size) {
    memmove(&get_key_slice()[start_pos - shift_size], &get_key_slice()[start_pos],
            sizeof(key_slice_type) * (key_slice_length - start_pos));
    memmove(&get_key_length()[start_pos - shift_size], &get_key_length()[start_pos],
            sizeof(key_length_type) * (key_slice_length - start_pos));
  }

  void shift_right_base_member(std::size_t start, std::size_t shift_size) {
    if (start >= key_slice_length - 1) {
      std::cerr << __FILE__ << " : " << __LINE__ << " : " << status::ERR_BOUNDARY  << std::endl;
      std::abort();
    } else if (shift_size == 0) {
      std::cerr << __FILE__ << " : " << __LINE__ << " : " << status::ERR_ARGUMENT  << std::endl;
      std::abort();
    }

    memmove(&get_key_slice()[start + shift_size], &get_key_slice()[start],
            sizeof(key_slice_type) * (key_slice_length - start - shift_size));
    memmove(&get_key_length()[start + shift_size], &get_key_length()[start],
            sizeof(key_length_type) * (key_slice_length - start - shift_size));
  }

  /**
   * @brief It unlocks this node.
   * @pre This node was already locked.
   * @return void
   */
  void version_unlock() &{
    version_.unlock();
  }

  void version_atomic_inc_vdelete() {
    version_.atomic_inc_vdelete();
  }

  void version_atomic_inc_vinsert() {
    version_.atomic_inc_vinsert();
  }

private:
  friend masstree_kvs;
  /**
   * @attention This variable is read/written concurrently.
   */
  alignas(CACHE_LINE_SIZE)
  node_version64 version_{};
  /**
   * @attention This member is protected by its parent's lock.
   * In the original paper, Fig 2 tells that parent's type is interior_node*,
   * however, at Fig 1, parent's type is interior_node or border_node both
   * interior's view and border's view.
   * This variable is read/written concurrently.
   */
  base_node *parent_{nullptr};
  /**
   * @attention This variable is read/written concurrently.
   */
  key_slice_type key_slice_[key_slice_length]{};
  /**
   * @attention This variable is read/written concurrently.
   * @details This is used for distinguishing the identity of link or value and same slices.
   * For example,
   * key 1 : \0, key 2 : \0\0, ... , key 8 : \0\0\0\0\0\0\0\0.
   * These keys have same key_slices (0) but different key_length.
   * If the length is more than 8, the lv points out to next layer.
   */
  key_length_type key_length_[key_slice_length]{};

/**
 * @details
 * Todo : It will be container to be able to switch database.
 */
  static std::atomic<base_node *> root_;
};

std::atomic<base_node *> base_node::root_ = nullptr;

} // namespace yakushima
