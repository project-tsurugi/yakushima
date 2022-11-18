/**
 * @file thread_info.h
 */

#pragma once

#include <atomic>

#include "clock.h"
#include "cpu.h"
#include "epoch.h"
#include "garbage_collection.h"

namespace yakushima {

class alignas(CACHE_LINE_SIZE) thread_info {
public:
    /**
   * @details Take the right to assign this gc_info.
   * @return true success.
   * @return false fail.
   */
    bool gain_the_right() {
        bool expected(running_.load(std::memory_order_acquire));
        for (;;) {
            if (expected) { return false; }
            if (running_.compare_exchange_weak(expected, true,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
                return true;
            }
        }
    }

    [[nodiscard]] Epoch get_begin_epoch() const {
        return begin_epoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] garbage_collection& get_gc_info() { return gc_info_; }

    [[nodiscard]] bool get_running() const {
        return running_.load(std::memory_order_acquire);
    }

    void set_begin_epoch(const Epoch epoch) {
        begin_epoch_.store(epoch, std::memory_order_relaxed);
    }

    void set_running(const bool tf) {
        running_.store(tf, std::memory_order_relaxed);
    }

private:
    /**
   * @details This is updated by worker and is read by leader. If the value is 0, 
   * it is invalid.
   */
    std::atomic<Epoch> begin_epoch_{0};
    std::atomic<bool> running_{false};
    garbage_collection gc_info_;
};

} // namespace yakushima