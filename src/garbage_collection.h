/**
 * @file garbage_collection.h
 */

#pragma once

#include <array>
#include <thread>
#include <utility>
#include <vector>

#include "base_node.h"
#include "cpu.h"

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

  void add_node_to_gc_container(Epoch gc_epoch, base_node *n) {
    node_container_->emplace_back(std::make_pair(gc_epoch, n));
  }

  void add_value_to_gc_container(Epoch gc_epoch, void *vp) {
    value_container_->emplace_back(std::make_pair(gc_epoch, vp));
  }

  /**
   * @tparam interior_node
   * @tparam border_node
   * @attention Use a template class so that the dependency does not cycle.
   */
  template<class interior_node, class border_node>
  static void fin() {
    struct S {
      static void parallel_worker(std::uint64_t left_edge, std::uint64_t right_edge) {
        for (std::size_t i = left_edge; i < right_edge; ++i) {
          auto &ncontainer = kGarbageNodes.at(i);
          for (auto itr = ncontainer.begin(); itr != ncontainer.end(); ++itr) {
            if (std::get<gc_target_index>(*itr)->get_version_border()) {
              delete dynamic_cast<border_node *>(std::get<gc_target_index>(*itr));
            } else {
              delete dynamic_cast<interior_node *>(std::get<gc_target_index>(*itr));
            }
          }
          ncontainer.clear();

          auto &vcontainer = kGarbageValues.at(i);
          for (auto itr = vcontainer.begin(); itr != vcontainer.end(); ++itr) {
            ::operator delete(std::get<gc_target_index>(*itr));
          }
          vcontainer.clear();
        }
      }
    };
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
      if (std::thread::hardware_concurrency() != 1) {
        if (i != std::thread::hardware_concurrency() - 1) {
          thv.emplace_back(S::parallel_worker,
                           YAKUSHIMA_MAX_PARALLEL_SESSIONS / std::thread::hardware_concurrency() * i,
                           YAKUSHIMA_MAX_PARALLEL_SESSIONS / std::thread::hardware_concurrency() * (i + 1));
        } else {
          thv.emplace_back(S::parallel_worker,
                           YAKUSHIMA_MAX_PARALLEL_SESSIONS / std::thread::hardware_concurrency() * i,
                           YAKUSHIMA_MAX_PARALLEL_SESSIONS);
        }
      } else {
        thv.emplace_back(S::parallel_worker, 0, YAKUSHIMA_MAX_PARALLEL_SESSIONS);
      }
    }

    for (auto &th : thv) {
      th.join();
    }
  }

  /**
   * @tparam interior_node
   * @tparam border_node
   * @attention Use a template class so that the dependency does not cycle.
   */
  template<class interior_node, class border_node>
  void gc() {
    gc_node<interior_node, border_node>();
    gc_value();
  }

  /**
   * @tparam interior_node
   * @tparam border_node
   * @attention Use a template class so that the dependency does not cycle.
   */
  template<class interior_node, class border_node>
  void gc_node() {
    Epoch gc_epoch = get_gc_epoch();
    auto gc_end_itr = node_container_->begin();
    for (auto itr = node_container_->begin(); itr != node_container_->end(); ++itr) {
      if (std::get<gc_epoch_index>(*itr) >= gc_epoch) {
        gc_end_itr = itr;
        break;
      } else {
        if (std::get<gc_target_index>(*itr)->get_version_border()) {
          delete dynamic_cast<border_node *>(std::get<gc_target_index>(*itr));
        } else {
          delete dynamic_cast<interior_node *>(std::get<gc_target_index>(*itr));
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

  static void init() {
    set_gc_epoch(0);
  }

  static void set_gc_epoch(Epoch epoch) {
    kGCEpoch.store(epoch, std::memory_order_release);
  }

  void set_node_container(std::vector<std::pair<Epoch, base_node *>> *container) {
    node_container_ = container;
  }

  void set_value_container(std::vector<std::pair<Epoch, void *>> *container) {
    value_container_ = container;
  }

  static Epoch get_gc_epoch() {
    return kGCEpoch.load(std::memory_order_acquire);
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
  static std::atomic<Epoch> kGCEpoch;

  std::vector<std::pair<Epoch, base_node *>> *node_container_{nullptr};
  std::vector<std::pair<Epoch, void *>> *value_container_{nullptr};
};

std::array<std::vector<std::pair<Epoch, base_node *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageNodes;
std::array<std::vector<std::pair<Epoch, void *>>, YAKUSHIMA_MAX_PARALLEL_SESSIONS> gc_container::kGarbageValues;
alignas(CACHE_LINE_SIZE)
std::atomic<Epoch> gc_container::kGCEpoch{0};

} // namespace yakushima