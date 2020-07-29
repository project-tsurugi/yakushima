/**
 * @file garbage_collection.h
 */

#pragma once

#include <array>
#include <thread>
#include <utility>
#include <vector>

#include "base_node.h"
#include "epoch.h"
#include "cpu.h"

namespace yakushima {

class gc_container {
public:
  class alignas(CACHE_LINE_SIZE) node_container {
  public:
    std::vector<std::pair<Epoch, base_node *>> &get_body() {
      return body_;
    }

  private:
    std::vector<std::pair<Epoch, base_node *>> body_;
  };

  class alignas(CACHE_LINE_SIZE) value_container {
  public:
    std::vector<std::tuple<Epoch, void *, std::size_t, std::align_val_t>> &get_body() {
      return body_;
    }

  private:
    std::vector<std::tuple<Epoch, void *, std::size_t, std::align_val_t>> body_;
  };

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
    node_container_->get_body().emplace_back(std::make_pair(gc_epoch, n));
  }

  void add_value_to_gc_container(Epoch gc_epoch, void *vp, std::size_t size, std::align_val_t alignment) {
    value_container_->get_body().emplace_back(std::make_tuple(gc_epoch, vp, size, alignment));
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
          for (auto &&elem : ncontainer.get_body()) {
            delete std::get<gc_target_index>(elem); // NOLINT
          }
          ncontainer.get_body().clear();

          auto &vcontainer = kGarbageValues.at(i);
          for (auto &&elem : vcontainer.get_body()) {
            ::operator delete(std::get<gc_target_index>(elem), std::get<gc_target_size_index>(elem),
                              std::get<gc_target_align_index>(elem));
          }
          vcontainer.get_body().clear();
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
    std::uint64_t g_ctr{0};
    for (auto &&itr : node_container_->get_body()) {
      if (std::get<gc_epoch_index>(itr) >= gc_epoch) {
        break;
      }
      delete std::get<gc_target_index>(itr); // NOLINT
      ++g_ctr;
    }
    if (g_ctr > 0) {
      node_container_->get_body().erase(node_container_->get_body().begin(),
                                        node_container_->get_body().begin() + g_ctr);
    }
  }

  void gc_value() {
    Epoch gc_epoch = get_gc_epoch();
    std::uint64_t g_ctr{0};
    for (auto &&itr : value_container_->get_body()) {
      if (std::get<gc_epoch_index>(itr) >= gc_epoch) {
        break;
      }
      ::operator delete(std::get<gc_target_index>(itr));
      ++g_ctr;
    }
    if (g_ctr > 0) {
      value_container_->get_body().erase(value_container_->get_body().begin(),
                                         value_container_->get_body().begin() + g_ctr);
    }
  }

  static void init() {
    set_gc_epoch(0);
  }

  static void set_gc_epoch(Epoch epoch) {
    kGCEpoch.store(epoch, std::memory_order_release);
  }

  void set_node_container(node_container *container) {
    node_container_ = container;
  }

  void set_value_container(value_container *container) {
    value_container_ = container;
  }

  static Epoch get_gc_epoch() {
    return kGCEpoch.load(std::memory_order_acquire);
  }

  node_container *get_node_container() {
    return node_container_;
  }

  value_container *get_value_container() {
    return value_container_;
  }

  static constexpr std::size_t gc_epoch_index = 0;
  static constexpr std::size_t gc_target_index = 1;
  static constexpr std::size_t gc_target_size_index = 2;
  static constexpr std::size_t gc_target_align_index = 3;
  static inline std::array<node_container, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageNodes; // NOLINT
  static inline std::array<value_container, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kGarbageValues; // NOLINT
  alignas(CACHE_LINE_SIZE) static inline std::atomic<Epoch> kGCEpoch; // NOLINT

private:
  node_container *node_container_{nullptr};
  value_container *value_container_{nullptr};
};

} // namespace yakushima