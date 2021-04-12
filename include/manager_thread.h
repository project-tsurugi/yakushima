/**
 * @file gc_info_table.h
 */

#pragma once

#include "gc_info_table.h"
#include <atomic>
#include <thread>

namespace yakushima {

class epoch_manager {
public:
    static void epoch_thread() {
        for (;;) {
            sleepMs(YAKUSHIMA_EPOCH_TIME);
            for (;;) {
                Epoch cur_epoch = epoch_management::get_epoch();
                bool verify{true};
                for (auto&& elem : gc_info_table::get_thread_info_table()) {
                    Epoch check_epoch = elem.get_begin_epoch();
                    if (check_epoch != 0 && check_epoch != cur_epoch) {
                        verify = false;
                        break;
                    }
                }
                if (verify) break;
                sleepMs(1);
            }
            epoch_management::epoch_inc();

            /**
             * attention : type of epoch is uint64_t
             */
            Epoch min_epoch(UINT64_MAX);
            for (auto&& elem : gc_info_table::get_thread_info_table()) {
                Epoch itr_epoch = elem.get_begin_epoch();
                if (itr_epoch != 0) {
                    /**
                     * itr_epoch is valid.
                     */
                    min_epoch = std::min(min_epoch, itr_epoch);
                }
            }
            if (min_epoch != UINT64_MAX && min_epoch) {
                gc_container::set_gc_epoch(min_epoch - 1);
            }
            if (kEpochThreadEnd.load(std::memory_order_acquire)) break;
        }
    }

    static void invoke_epoch_thread() {
        kEpochThread = std::thread(epoch_thread);
    }

    static void join_epoch_thread() {
        kEpochThread.join();
    }

    static void set_epoch_thread_end() {
        kEpochThreadEnd.store(true, std::memory_order_release);
    }

private:
    static inline std::thread kEpochThread;                                  // NOLINT : can't become constexpr
    alignas(CACHE_LINE_SIZE) static inline std::atomic<bool> kEpochThreadEnd;// NOLINT : can't become constexpr
};

}// namespace yakushima