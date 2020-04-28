/**
 * @file mt_version.h
 * @brief version number layout
 */

#pragma once

#include <cstdint>

namespace yakushima {

enum class node_version_params32 : std::uint32_t {
  lock_bit = (1U << 0),
  inserting_bit = (1U << 1),
  splitting_bit = (1U << 2),
  dirty_mask = inserting_bit | splitting_bit, // use for stableversion
  unlock_mask = lock_bit | inserting_bit | splitting_bit, // use for unlock
  deleted_bit = (1U << 3),
  root_bit = (1U << 4),
  leaf_bit = (1U << 5),
  unused_bit = (1U << 6),
  vinsert_lowbit = (1U << 7),
  vinsert_topbit = (1U << 14), // vinsert -> 8 bits
  vsplit_lowbit = (1U << 15),
  vsplit_topbit = (1U << 31) // vsplit -> 17 bits
};

class node_version32 {
public:
  node_version32() : version_(0) {}

private:
  uint32_t version_;
};

enum class node_version_params64 : std::uint64_t {
  lock_bit = (1ull << 0),
  inserting_bit = (1ull << 1),
  splitting_bit = (1ull << 2),
  dirty_mask = inserting_bit | splitting_bit, // use for stableversion
  unlock_mask = lock_bit | inserting_bit | splitting_bit, // use for unlock
  deleted_bit = (1ull << 3),
  root_bit = (1ull << 4),
  leaf_bit = (1ull << 5),
  unused_bit = (1ull << 6),
  vinsert_lowbit = (1ull << 7),
  vinsert_topbit = (1ull << 22), // vinsert -> 16 bits
  vsplit_lowbit = (1ull << 23),
  vsplit_topbit = (1ull << 63) // vsplit -> 41 bits
};

class node_version64 {
public:
  node_version64() : version_(0) {}
private:
  uint64_t version_;
};
} // namespace yakushima