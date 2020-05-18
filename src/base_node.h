/**
 * @file mt_base_node.h
 */

#pragma once

#include "atomic_wrapper.h"
#include "cpu.h"
#include "scheme.h"
#include "version.h"

namespace yakushima {

class base_node {
public:
  static constexpr std::size_t key_slice_length = 15;
  using key_slice_type = std::uint64_t;

  base_node() = default;

  ~base_node() = default;

  /**
   * A virtual function is defined because It wants to distinguish the child class of the contents
   * by using polymorphism.
   * So this function is pure virtual function.
   */
  virtual status destroy() = 0;

  /**
   * copy/move assign/constructor can not declare due to atomic member @a parent_
   */

  [[nodiscard]] uint64_t *get_key_slice() {
    return key_slice_;
  }

  [[nodiscard]] uint64_t get_key_slice_at(std::size_t index) {
    if (index > static_cast<std::size_t>(key_slice_length)) {
      std::abort();
    }
    return loadAcquire(key_slice_[index]);
  }

  [[nodiscard]] bool get_lock() & {
    return version_.get_locked();
  }

  [[nodiscard]] node_version64_body get_stable_version() &{
    return version_.get_stable_version();
  }

  [[nodiscard]] base_node *get_parent() &{
    return loadAcquire(parent_);
  }

  [[nodiscard]] node_version64 get_version() &{
    return version_;
  }

  [[nodiscard]] bool get_version_border() {
    return version_.get_border();
  }

  [[nodiscard]] std::size_t get_version_vsplit() {
    return version_.get_vsplit();
  }

  void init_base() {
    version_.init();
    set_parent(nullptr);
    for (std::size_t i = 0; i < key_slice_length; ++i) {
      key_slice_[i] = 0;
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
        p->unlock();
        p = check;
      }
    }
  }

  void set_key_slice(std::size_t index, std::uint64_t key_slice) {
    if (index >= key_slice_length) std::abort();
    key_slice_[index] = key_slice;
  }

  void set_parent(base_node* new_parent) {
    storeRelease(parent_, new_parent);
  }

  void set_version_border(bool tf) {
    version_.set_border(tf);
  }

  void set_version_root(bool tf) {
    version_.set_root(tf);
  }

  /**
   * @brief It unlocks this node.
   * @pre This node was already locked.
   * @return void
   */
  void unlock() &{
    version_.unlock();
  }


private:
  /**
   * @attention This variable is read/written concurrently.
   */
  alignas(CACHE_LINE_SIZE)
  node_version64 version_{};
  /**
   * @details Member parent_ is a type "std::atomic<interior_node*>" essentially,
   * but declare a type "std::atomic<base_node*>" due to including file order.
   * @attention This member is protected by its parent's lock.
   * In the original paper, Fig 2 tells that parent's type is interior_node*,
   * however, at Fig 1, parent's type is interior_node or border_node both
   * interior's view and border's view.
   * This variable is read/written concurrently.
   */
  base_node * parent_{nullptr};
  /**
   * @attention This variable is read/written concurrently.
   */
  key_slice_type key_slice_[key_slice_length]{};
};

} // namespace yakushima
