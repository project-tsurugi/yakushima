/**
 * @file thread_info_table.h
 */

#pragma once

#include "border_node.h"
#include "config.h"
#include "interior_node.h"
#include "thread_info.h"

namespace yakushima {

class thread_manager {
public:
    static bool check_room() {
        std::size_t expected{
            threads_num_.load(std::memory_order_acquire)};
        for (;;) {
            if (expected >= std::thread::hardware_concurrency()) {
                return false;
            }
            std::size_t desired = expected + 1;
            if (threads_num_.compare_exchange_weak(
                    expected, desired, std::memory_order_release,
                    std::memory_order_acquire)) {
                return true;
            }
        }
    }

    static void return_room() { threads_num_ -= 1; }

private:
    inline static std::atomic<std::size_t> threads_num_{0};  // NOLINT
};

class thread_info_table {
public:
    /**
     * @brief Allocates a free session.
     * @param[out] token If the return value of the function is status::OK,
     * then the token is the acquired session.
     * @return status::OK success.
     * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up
     * and running.
     */
    static status assign_thread_info(Token& token) {
        for (auto&& elem : thread_info_table_) {
            if (elem.gain_the_right()) {
                elem.set_begin_epoch(epoch_management::get_epoch());
                token = &(elem);
                return status::OK;
            }
        }
        return status::WARN_MAX_SESSIONS;
    }

    static void fin() {
        std::vector<std::thread> th_vc;
        th_vc.reserve(thread_info_table_.size());
        for (auto&& elem : thread_info_table_) {
            auto process = [&elem](bool do_rr) {
                elem.get_gc_info().fin<interior_node, border_node>();
                if (do_rr) { thread_manager::return_room(); }
            };
            if (thread_manager::check_room()) {
                th_vc.emplace_back(process, true);
            } else {
                process(false);
            }
        }
        for (auto&& th : th_vc) { th.join(); }
    }

    static void gc() {
        for (auto&& elem : thread_info_table_) {
            elem.get_gc_info().gc<interior_node, border_node>();
        }
    }

    /**
     * @brief Get reference of thread_info_table_.
     * @return the reference of thread_info_table_.
     */
    static std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS>&
    get_thread_info_table() {
        return thread_info_table_;
    }

    /**
     * @brief initialize thread_info_table_.
     * @pre global epoch is not yet functional because it assigns 0 to begin_epoch as
     * the initial value.
     * @return void
     */
    static void init() {
        for (auto&& elem : thread_info_table_) {
            elem.set_begin_epoch(0);
            elem.set_running(false);
        }
    }

    /**
     * @tparam interior_node Class information is given at compile time to eliminate
     * the dependency between header files.
     * @tparam border_node Class information is given at compile time to eliminate the
     * dependency between header files.
     * @param[in] token Session information. The behavior is undefined if the @a token is invalid.
     * @return status::OK success.
     */
    template<class interior_node, class border_node>
    static status leave_thread_info(Token token) {
        auto* target = static_cast<thread_info*>(token);
        target->set_begin_epoch(0);
        target->set_running(false);
        return status::OK;
    }

private:
    /**
     * @brief Session information used by garbage collection.
     */
    static inline std::array<thread_info,                     // NOLINT
                             YAKUSHIMA_MAX_PARALLEL_SESSIONS> // NOLINT
            thread_info_table_;                               // NOLINT
};

} // namespace yakushima
