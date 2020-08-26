/**
 * @file gc_info.h
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iterator>
#include <mutex>
#include <thread>

#include "clock.h"
#include "cpu.h"
#include "epoch.h"
#include "garbage_collection.h"
#include "scheme.h"

namespace yakushima {

class alignas(CACHE_LINE_SIZE) gc_info {
public:
  /**
   * @details Take the right to assign this gc_info.
   * @return true success.
   * @return false fail.
   */
  bool gain_the_right() {
    bool expected(running_.load(std::memory_order_acquire));
    for (;;) {
      if (expected) return false;
      if (running_.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return true;
      }
    }
  }

  template<class interior_node, class border_node>
  void gc() {
    gc_container_.gc<interior_node, border_node>();
  }

  [[nodiscard]] Epoch get_begin_epoch() const {
    return begin_epoch_.load(std::memory_order_acquire);
  }

  [[nodiscard]] gc_container::node_container *get_node_container() const {
    return gc_container_.get_node_container();
  }

  [[nodiscard]] bool get_running() const {
    return running_.load(std::memory_order_acquire);
  }

  [[nodiscard]] gc_container::value_container *get_value_container() const {
    return gc_container_.get_value_container();
  }

  void lock_epoch() {
    lock_epoch_.lock();
  }

  void move_node_to_gc_container(base_node *const n) {
    gc_container_.add_node_to_gc_container(begin_epoch_.load(std::memory_order_acquire), n);
  }

  void move_value_to_gc_container(void *const vp, const std::size_t size, const std::align_val_t alignment) {
    gc_container_.add_value_to_gc_container(begin_epoch_.load(std::memory_order_acquire), vp, size, alignment);
  }

  void set_begin_epoch(const Epoch epoch) {
    begin_epoch_.store(epoch, std::memory_order_relaxed);
  }

  void set_gc_container(const std::size_t index) {
    gc_container_.set(index);
  }

  void set_running(const bool tf) {
    running_.store(tf, std::memory_order_relaxed);
  }

  bool try_lock_epoch() {
    return lock_epoch_.try_lock();
  }

  void unlock_epoch() {
    lock_epoch_.unlock();
  }

private:

  /**
   * @details This is updated by worker and is read by leader.
   */
  std::atomic<Epoch> begin_epoch_{0};
  /**
   * @details This is used for operating begin_epoch_. Coordinating between enter/leave session.
   */
  std::mutex lock_epoch_{};
  gc_container gc_container_;
  std::atomic<bool> running_{false};
};

} // namespace yakushima