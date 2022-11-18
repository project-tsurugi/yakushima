/**
 * @file thread_info_table.h
 */

#pragma once

#include "border_node.h"
#include "config.h"
#include "interior_node.h"
#include "thread_info.h"

namespace yakushima {

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
                if (do_rr) { destroy_manager::return_room(); }
            };
            if (destroy_manager::check_room()) {
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
   * @details When @a token points to an invalid memory location, an error occurs 
   * if @a token is referenced. 
   * To avoid this, it scans the table. 
   * So if @token is invalid one, return status::WARN_INVALID_TOKEN.
   * @tparam interior_node Class information is given at compile time to eliminate 
   * the dependency between header files.
   * @tparam border_node Class information is given at compile time to eliminate the
   * dependency between header files.
   * @param[in] token Session information.
   * @return status::OK success.
   * @return status::WARN_INVALID_TOKEN The @a token of the argument was invalid.
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