/**
 * @file thread_info_table.h
 */

#pragma once

#include "thread_info.h"

namespace yakushima {

class thread_info_table {
public:
    /**
     * @brief Allocates a free session.
     * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
     * @return status::OK success.
     * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
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

    /**
     * @brief Get reference of thread_info_table_.
     * @return the reference of thread_info_table_.
     */
    static std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS>& get_thread_info_table() {
        return thread_info_table_;
    }

    /**
     * @brief initialize thread_info_table_.
     * @pre global epoch is not yet functional because it assigns 0 to begin_epoch as the initial value.
     * @return void
     */
    static void init() {
        for (auto itr = thread_info_table_.begin(); itr != thread_info_table_.end(); ++itr) {// NOLINT
            itr->set_begin_epoch(0);
            itr->set_running(false);
        }
    }

    /**
     * @details When @a token points to an invalid memory location, an error occurs if @a token is referenced.
     * To avoid this, it scans the table. So if @token is invalid one, return status::WARN_INVALID_TOKEN.
     * @tparam interior_node Class information is given at compile time to eliminate the dependency between header files.
     * @tparam border_node Class information is given at compile time to eliminate the dependency between header files.
     * @param[in] token Session information.
     * @return status::OK success.
     * @return status::WARN_INVALID_TOKEN The @a token of the argument was invalid.
     */
    template<class interior_node, class border_node>
    static status leave_thread_info(Token token) {
        auto target = static_cast<thread_info*>(token);
        target->set_begin_epoch(0);
        target->set_running(false);
        return status::OK;
    }

private:
    /**
     * @brief Session information used by garbage collection.
     */
    static inline std::array<thread_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> thread_info_table_;// NOLINT
};

}// namespace yakushima