/**
 * @file garbage_collection.h
 */

#pragma once

#include <array>
#include <thread>
#include <utility>
#include <vector>

#include "base_node.h"
#include "concurrent_queue.h"
#include "cpu.h"
#include "epoch.h"

namespace yakushima {

class garbage_collection {
public:
    /**
   * @tparam interior_node
   * @tparam border_node
   * @attention Use a template class so that the dependency does not cycle.
   */
    template<class interior_node, class border_node>
    void fin() {
        // for cache
        if (std::get<gc_target_index>(cache_node_container_) != nullptr) {
            delete std::get<gc_target_index>(cache_node_container_); // NOLINT
            std::get<gc_target_index>(cache_node_container_) = nullptr;
        }

        while (!node_container_.empty()) {
            std::tuple<Epoch, base_node*> elem;
            while (!node_container_.try_pop(elem)) continue;
            delete std::get<gc_target_index>(elem); // NOLINT
        }

        // for cache
        if (std::get<gc_target_index>(cache_value_container_) != nullptr) {
            ::operator delete(
                    std::get<gc_target_index>(cache_value_container_),
                    std::get<gc_target_size_index>(cache_value_container_),
                    std::get<gc_target_align_index>(cache_value_container_));
            std::get<gc_target_index>(cache_value_container_) = nullptr;
        }

        while (!value_container_.empty()) {
            std::tuple<Epoch, void*, std::size_t, std::align_val_t> elem;
            while (!value_container_.try_pop(elem)) continue;
            ::operator delete(std::get<gc_target_index>(elem),
                              std::get<gc_target_size_index>(elem),
                              std::get<gc_target_align_index>(elem));
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

        // for cache
        if (std::get<gc_target_index>(cache_node_container_) != nullptr) {
            if (std::get<gc_epoch_index>(cache_node_container_) >= gc_epoch) {
                return;
            }
            delete std::get<gc_target_index>(cache_node_container_); // NOLINT
            std::get<gc_target_index>(cache_node_container_) = nullptr;
        }

        // for container
        while (!node_container_.empty()) {
            std::tuple<Epoch, base_node*> elem;
            while (!node_container_.try_pop(elem)) continue;
            if (std::get<gc_epoch_index>(elem) >= gc_epoch) {
                cache_node_container_ = elem;
                return;
            }
            delete std::get<gc_target_index>(elem); // NOLINT
        }
    }

    void gc_value() {
        Epoch gc_epoch = get_gc_epoch();

        if (std::get<gc_target_index>(cache_value_container_) != nullptr) {
            if (std::get<gc_epoch_index>(cache_value_container_) >= gc_epoch) {
                return;
            }
            ::operator delete(
                    std::get<gc_target_index>(cache_value_container_),
                    std::get<gc_target_size_index>(cache_value_container_),
                    std::get<gc_target_align_index>(cache_value_container_));
            std::get<gc_target_index>(cache_value_container_) = nullptr;
        }

        while (!value_container_.empty()) {
            std::tuple<Epoch, void*, std::size_t, std::align_val_t> elem;
            if (!value_container_.try_pop(elem)) { continue; }
            if (std::get<gc_epoch_index>(elem) >= gc_epoch) {
                cache_value_container_ = elem;
                return;
            }
            ::operator delete(std::get<gc_target_index>(elem),
                              std::get<gc_target_size_index>(elem),
                              std::get<gc_target_align_index>(elem));
        }
    }

    static Epoch get_gc_epoch() {
        return gc_epoch_.load(std::memory_order_acquire);
    }

    void push_node_container(std::tuple<Epoch, base_node*> elem) {
        node_container_.push(elem);
    }

    void push_value_container(
            std::tuple<Epoch, void*, std::size_t, std::align_val_t> elem) {
        value_container_.push(elem);
    }

    static void set_gc_epoch(const Epoch epoch) {
        gc_epoch_.store(epoch, std::memory_order_release);
    }

private:
    static constexpr std::size_t gc_epoch_index = 0;
    static constexpr std::size_t gc_target_index = 1;
    static constexpr std::size_t gc_target_size_index = 2;
    static constexpr std::size_t gc_target_align_index = 3;
    alignas(CACHE_LINE_SIZE) static inline std::atomic<Epoch> gc_epoch_{
            0};                                                      // NOLINT
    std::tuple<Epoch, base_node*> cache_node_container_{0, nullptr}; // NOLINT
    concurrent_queue<std::tuple<Epoch, base_node*>> node_container_; // NOLINT
    std::tuple<Epoch, void*, std::size_t, std::align_val_t>
            cache_value_container_{0, nullptr, 0,
                                   static_cast<std::align_val_t>(0)}; // NOLINT
    concurrent_queue<std::tuple<Epoch, void*, std::size_t, std::align_val_t>>
            value_container_; // NOLINT
};

} // namespace yakushima