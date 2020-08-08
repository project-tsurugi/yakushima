/**
 * @file session_info.h
 */

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iterator>
#include <thread>

#include "clock.h"
#include "cpu.h"
#include "epoch.h"
#include "garbage_collection.h"
#include "scheme.h"

namespace yakushima {

class alignas(CACHE_LINE_SIZE) session_info {
public:
  /**
   * @details Take the right to assign this session_info.
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

  Epoch get_begin_epoch() {
    return begin_epoch_.load(std::memory_order_acquire);
  }

  gc_container::node_container *get_node_container() {
    return gc_container_.get_node_container();
  }

  bool get_running() {
    return running_.load(std::memory_order_acquire);
  }

  gc_container::value_container *get_value_container() {
    return gc_container_.get_value_container();
  }

  void move_node_to_gc_container(base_node *n) {
    gc_container_.add_node_to_gc_container(begin_epoch_.load(std::memory_order_acquire), n);
  }

  void move_value_to_gc_container(void *vp, std::size_t size, std::align_val_t alignment) {
    gc_container_.add_value_to_gc_container(begin_epoch_.load(std::memory_order_acquire), vp, size, alignment);
  }

  void set_begin_epoch(Epoch epoch) {
    begin_epoch_.store(epoch, std::memory_order_relaxed);
  }

  void set_gc_container(std::size_t index) {
    gc_container_.set(index);
  }

  void set_running(bool tf) {
    running_.store(tf, std::memory_order_relaxed);
  }

private:

  /**
   * @details This is updated by worker and is read by leader.
   */
  std::atomic<Epoch> begin_epoch_{0};
  gc_container gc_container_;
  std::atomic<bool> running_{false};
};

} // namespace yakushima