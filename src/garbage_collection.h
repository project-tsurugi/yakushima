/**
 * @file garbage_collection.h
 */

#pragma once

#include <array>
#include <vector>

#include "base_node.h"
#include "border_node.h"
#include "cpu.h"
#include "interior_node.h"

namespace yakushima {

class gc_container {
public:
  void set(std::size_t index) {
    try {
      set_node_container(&kGarbageNodes.at(index));
      set_value_container(&kGarbageValues.at(index));
    } catch (std::out_of_range &e) {
      std::cout << __FILE__ << " : " << __LINE__ << std::endl;
    } catch (...) {
      std::cout << __FILE__ << " : " << __LINE__ << std::endl;
    }
  }

  void set_node_container(std::vector<base_node *> *container) {
    node_container_ = container;
  }

  void set_value_container(std::vector<void *> *container) {
    value_container_ = container;
  }

  std::vector<base_node *> *get_node_container() {
    return node_container_;
  }

  std::vector<void *> *get_value_container() {
    return value_container_;
  }

private:
  static std::array<std::vector<base_node *>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageNodes;
  static std::array<std::vector<void *>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageValues;

  std::vector<base_node *> *node_container_{nullptr};
  std::vector<void *> *value_container_{nullptr};
};

alignas(CACHE_LINE_SIZE)
std::array<std::vector<base_node *>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageNodes;
std::array<std::vector<void *>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageValues;

} // namespace yakushima