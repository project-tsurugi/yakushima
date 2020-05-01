/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <atomic>
#include <bitset>
#include <cmath>
#include <cstdint>

#include "atomic_wrapper.h"
#include "cpu.h"

namespace yakushima {

class node_version64 {
public:
  void set(node_version64 newv) & {
    storeRelease(&obj_, (newv.get_obj()));
  }

  std::uint64_t get_obj() const {
    return loadAcquire(obj_);
  }

private:
  union {
    std::uint64_t obj_;
    struct {
      bool locked: 1;
      bool inserting: 1;
      bool splitting: 1;
      bool deleted: 1;
      bool root: 1;
      bool leaf: 1;
      std::uint32_t vinsert: 16;
      std::uint64_t vsplit: 41;
      bool unused: 1;
    };
  };
};

} // namespace yakushima