/**
 * @file epoch.h
 */

#pragma once

#include <atomic>

namespace yakushima {

using Epoch = std::uint64_t;

class epoch_management {
public:
    static void epoch_inc() { epoch_.fetch_add(1); }

    static Epoch get_epoch() { return epoch_.load(std::memory_order_acquire); }

private:
    /**
   * @todo consider wrap around. Wrap around after 23,397,696,694 days.
   */
    static inline std::atomic<Epoch> epoch_{1}; // NOLINT
};

} // namespace yakushima