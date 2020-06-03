/**
 * @file garbage_collection.h
 */

#pragma once

#include <vector>

#include "base_node.h"
#include "border_node.h"
#include "cpu.h"
#include "interior_node.h"

namespace yakushima {

class gc_container {
public:
  void set(std::size_t index) {
    node_container = &kGarbageNodes[index];
    value_container = &kGarbageValues[index];
  }

private:
  static std::vector<base_node *> kGarbageNodes[YAKUSHIMA_MAX_PARALLEL_SESSIONS];
  static std::vector<void *> kGarbageValues[YAKUSHIMA_MAX_PARALLEL_SESSIONS];

  std::vector<base_node *> *node_container;
  std::vector<void *> *value_container;
};

alignas(CACHE_LINE_SIZE)
std::vector<base_node *> gc_container::kGarbageNodes[YAKUSHIMA_MAX_PARALLEL_SESSIONS];
std::vector<void *> gc_container::kGarbageValues[YAKUSHIMA_MAX_PARALLEL_SESSIONS];

} // namespace yakushima