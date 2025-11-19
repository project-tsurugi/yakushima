/**
 * @file tree_instance.h
 */

#pragma once

#include <atomic>
#include <xmmintrin.h>

#include "atomic_wrapper.h"
#include "clock.h"

namespace yakushima {

class base_node;

class alignas(CACHE_LINE_SIZE) tree_instance {
public:
    bool cas_root_ptr(base_node** expected, base_node** desired) {
        return weakCompareExchange(&root_, expected, desired);
    }

    bool empty() { return load_root_ptr() == nullptr; }

    base_node* load_root_ptr() { return loadAcquireN(root_); }

    void store_root_ptr(base_node* new_root) {
        return storeReleaseN(root_, new_root);
    }

    void root_lock() {
        bool expected{};
        bool desired{};
        for (;;) {
            for (size_t i = 1;; ++i) {
                expected = root_lock_.load(std::memory_order_acquire);
                if (expected) {
                    if (i >= 10) { break; }
                    _mm_pause();
                    continue;
                }
                desired = true;
                if (root_lock_.compare_exchange_weak(expected, desired,
                                                std::memory_order_acq_rel,
                                                std::memory_order_acquire)) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }

    void root_unlock() {
        root_lock_.store(false, std::memory_order_release);
    }

private:
    base_node* root_{nullptr};

    std::atomic_bool root_lock_{false};
};

} // namespace yakushima
