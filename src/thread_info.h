/**
 * @file thread_info.h
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

class alignas(CACHE_LINE_SIZE) thread_info {
public:
  /**
   * @brief Allocates a free session.
   * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
   * @return status::OK success.
   * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
   */
  static status assign_session(Token &token) {
    for (auto &&elem : kThreadInfoTable) {
      if (elem.gain_the_right()) {
        elem.set_begin_epoch(epoch_management::get_epoch());
        token = static_cast<void *>(&(elem));
        return status::OK;
      }
    }
    return status::WARN_MAX_SESSIONS;
  }

  static void epoch_thread() {
    for (;;) {
      sleepMs(YAKUSHIMA_EPOCH_TIME);
      epoch_management::epoch_inc();

      /**
       * attention : type of epoch is uint64_t
       */
      Epoch min_epoch(UINT64_MAX);
      for (auto &&elem : kThreadInfoTable) {
        Epoch itr_epoch = elem.get_begin_epoch();
        if (itr_epoch != 0) {
          /**
           * itr_epoch is valid.
           */
          min_epoch = std::min(min_epoch, itr_epoch);
        }
      }
      if (min_epoch != UINT64_MAX) {
        gc_container::set_gc_epoch(min_epoch);
      }
      if (kEpochThreadEnd.load(std::memory_order_acquire)) break;
    }
  }

  /**
   * @pre global epoch is not yet functional because it assigns 0 to begin_epoch as the initial value.
   */
  static void init() {
    for (auto itr = kThreadInfoTable.begin(); itr != kThreadInfoTable.end(); ++itr) {
      itr->set_begin_epoch(0);
      itr->set_gc_container(std::distance(kThreadInfoTable.begin(), itr));
      itr->set_running(false);
    }
  }

  static void invoke_epoch_thread() {
    kEpochThread = std::thread(epoch_thread);
  }

  static void join_epoch_thread() {
    kEpochThread.join();
  }

  /**
   * @details When @a token points to an invalid memory location, an error occurs if @a token is referenced.
   * To avoid this, it scans the table.
   * @tparam interior_node
   * @tparam border_node
   * @param token
   * @return
   */
  template<class interior_node, class border_node>
  static status leave_session(Token &token) {
    for (auto &&elem : kThreadInfoTable) {
      if (token == static_cast<void *>(&(elem))) {
        elem.gc_container_.gc<interior_node, border_node>();
        elem.set_running(false);
        elem.set_begin_epoch(0);
        return status::OK;
      }
    }
    return status::WARN_INVALID_TOKEN;
  }

  /**
   * @details Take the right to assign this thread_info.
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

  void move_value_to_gc_container(void *vp) {
    gc_container_.add_value_to_gc_container(begin_epoch_.load(std::memory_order_acquire), vp);
  }

  void set_begin_epoch(Epoch epoch) {
    begin_epoch_.store(epoch, std::memory_order_relaxed);
  }

  static void set_epoch_thread_end() {
    kEpochThreadEnd.store(true, std::memory_order_release);
  }

  void set_gc_container(std::size_t index) {
    gc_container_.set(index);
  }

  void set_running(bool tf) {
    running_.store(tf, std::memory_order_relaxed);
  }

private:
  static std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kThreadInfoTable;
  static std::thread kEpochThread;
  static std::atomic<bool> kEpochThreadEnd;

  /**
   * @details This is updated by worker and is read by leader.
   */
  std::atomic<Epoch> begin_epoch_{0};
  gc_container gc_container_;
  std::atomic<bool> running_{false};
};

std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> thread_info::kThreadInfoTable;
std::thread thread_info::kEpochThread;
alignas(CACHE_LINE_SIZE)
std::atomic<bool> thread_info::kEpochThreadEnd{false};

} // namespace yakushima