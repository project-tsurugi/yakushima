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
  static status assign_session(Token &token) {
    for (auto itr = kThreadInfoTable.begin(); itr != kThreadInfoTable.end(); ++itr) {
      if (itr->gain_the_right()) {
        token = static_cast<void *>(&(*itr));
        return status::OK;
      }
    }
    return status::WARN_MAX_SESSIONS;
  }

  /**
   * @details When @a token points to an invalid memory location, an error occurs if @a token is referenced.
   * To avoid this, it scans the table.
   * @todo performance improvement.
   * @param token
   * @return
   */
  static status leave_session(Token &token) {
    for (auto itr = kThreadInfoTable.begin(); itr != kThreadInfoTable.end(); ++itr) {
      if (token == static_cast<void*>(&(*itr))) {
        /**
         * todo : gc
         */
        itr->set_running(false);
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

  std::vector<base_node *> *get_node_container() {
    return gc_container_.get_node_container();
  }

  bool get_running() {
    return running_.load(std::memory_order_acquire);
  }

  std::vector<void *> *get_value_container() {
    return gc_container_.get_value_container();
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
  std::atomic<Epoch> begin_epoch_{0};
  gc_container gc_container_;
  std::atomic<bool> running_{false};
};

std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> thread_info::kThreadInfoTable;

} // namespace yakushima