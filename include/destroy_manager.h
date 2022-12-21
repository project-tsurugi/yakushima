/**
 * @file destroy_manager.h
 */
#pragma once

#include <atomic>
#include <thread>

namespace yakushima {

class destroy_manager {
public:
    static bool check_room() {
        std::size_t expected{
                destroy_threads_num_.load(std::memory_order_acquire)};
        for (;;) {
            if (expected >= std::thread::hardware_concurrency()) {
                return false;
            }
            std::size_t desired = expected + 1;
            if (destroy_threads_num_.compare_exchange_weak(
                        expected, desired, std::memory_order_release,
                        std::memory_order_acquire)) {
                return true;
            }
        }
    }

    static std::size_t get_threads_num() {
        return destroy_threads_num_.load(std::memory_order_acquire);
    }

    static void return_room() { destroy_threads_num_ -= 1; }

private:
    static inline std::atomic<std::size_t> destroy_threads_num_{0};
};

} // namespace yakushima