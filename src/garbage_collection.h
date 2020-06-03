/**
 * @file garbage_collection.h
 */

#pragma once

#include <array>
#include <utility>
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

  void gc() {
    gc_node();
    gc_value();
  }

  void gc_node() {
    Epoch gc_epoch = get_gc_epoch();
    auto gc_end_itr = node_container_->begin();
    for (auto itr = node_container_->begin(); itr != node_container_->end(); ++itr) {
      if (std::get<gc_epoch_index>(*itr) >= gc_epoch) {
        gc_end_itr = itr;
        break;
      } else {
        if (typeid(*std::get<gc_target_index>(*itr)) == typeid(border_node)) {
          delete dynamic_cast<border_node*>(std::get<gc_target_index>(*itr));
        } else if (typeid(*std::get<gc_target_index>(*itr)) == typeid(interior_node)) {
          delete dynamic_cast<interior_node*>(std::get<gc_target_index>(*itr));
        } else {
          /**
           * unreachable point.
           */
          std::cerr << __FILE__ << ": " << __LINE__ << std::endl;
          std::abort();
        }
      }
    }
    if (std::distance(node_container_->begin(), gc_end_itr) > 0) {
      node_container_->erase(node_container_->begin(), gc_end_itr);
    }
  }

  void gc_value() {
    Epoch gc_epoch = get_gc_epoch();
    auto gc_end_itr = value_container_->begin();
    for (auto itr = value_container_->begin(); itr != value_container_->end(); ++itr) {
      if (std::get<gc_epoch_index>(*itr) >= gc_epoch) {
        gc_end_itr = itr;
        break;
      } else {
        ::operator delete(std::get<gc_target_index>(*itr));
      }
    }
    if (std::distance(value_container_->begin(), gc_end_itr) > 0) {
      value_container_->erase(value_container_->begin(), gc_end_itr);
    }
  }

  static void set_gc_epoch(Epoch epoch) {
    kGarbageCollectionEpoch.store(epoch, std::memory_order_release);
  }

  void set_node_container(std::vector<std::pair<Epoch, base_node *>> *container) {
    node_container_ = container;
  }

  void set_value_container(std::vector<std::pair<Epoch, void *>> *container) {
    value_container_ = container;
  }

  static Epoch get_gc_epoch() {
    return kGarbageCollectionEpoch.load(std::memory_order_acquire);
  }

  std::vector<std::pair<Epoch, base_node *>> *get_node_container() {
    return node_container_;
  }

  std::vector<std::pair<Epoch, void *>> *get_value_container() {
    return value_container_;
  }

private:
  static constexpr std::size_t gc_epoch_index = 0;
  static constexpr std::size_t gc_target_index = 1;
  static std::array<std::vector<std::pair<Epoch, base_node *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageNodes;
  static std::array<std::vector<std::pair<Epoch, void *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageValues;
  static std::atomic<Epoch> kGarbageCollectionEpoch;

  std::vector<std::pair<Epoch, base_node *>> *node_container_{nullptr};
  std::vector<std::pair<Epoch, void *>> *value_container_{nullptr};
};

alignas(CACHE_LINE_SIZE)
std::array<std::vector<std::pair<Epoch, base_node *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageNodes;
std::array<std::vector<std::pair<Epoch, void *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageValues;
std::atomic<Epoch> gc_container::kGarbageCollectionEpoch{0};

} // namespace yakushima