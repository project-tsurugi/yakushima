/**
 * @file epoch.h
 */

#pragma once

#include "scheme.h"

namespace yakushima {

using Epoch = std::uint64_t;

class epoch_management {
public:
  static Epoch get_epoch() {
    return kEpoch.load(std::memory_order_acquire);
  }

  static void epoch_inc() {
    kEpoch.fetch_add(1);
  }

private:
  static std::atomic<Epoch> kEpoch;
};

/**
 * @todo consider wrap around. Wrap around after 23,397,696,694 days.
 */
std::atomic<Epoch> epoch_management::kEpoch{0};

} //