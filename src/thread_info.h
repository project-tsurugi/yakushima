/**
 * @file thread_info.h
 */

#pragma once

#include <array>
#include <atomic>
#include <iterator>

#include "cpu.h"
#include "garbage_collection.h"
#include "scheme.h"

namespace yakushima {

class thread_info {
public:
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

  /**
   * @brief Allocates a free session.
   * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
   * @return status::OK success.
   * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
   */
  status assing_session(Token &token) {
    for (auto itr = kThreadInfoTable.begin(); itr != kThreadInfoTable.end(); ++itr) {
      if (gain_the_right()) {
        token = static_cast<void*>(&(*itr));
        return status::OK;
      }
    }
    return status::WARN_MAX_SESSIONS;
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
  static std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kThreadInfoTable;

  /**
   * @details This is updated by worker and is read by leader.
   */
  alignas(CACHE_LINE_SIZE)
  std::atomic<Epoch> begin_epoch_;
  gc_container gc_container_;
  std::atomic<bool> running_;
};

std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> thread_info::kThreadInfoTable;

} // namespace yakushima