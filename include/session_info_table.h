/**
 * @file session_info_table.h
 */

#pragma once

#include "session_info.h"

namespace yakushima {

class session_info_table {
public:
  /**
   * @brief Allocates a free session.
   * @param[out] token If the return value of the function is status::OK, then the token is the acquired session.
   * @return status::OK success.
   * @return status::WARN_MAX_SESSIONS The maximum number of sessions is already up and running.
   */
  static status assign_session(Token &token) {
    for (auto &&elem : kThreadInfoTable) {
      if (elem.gain_the_right()) {
        elem.lock_epoch();
        elem.set_begin_epoch(epoch_management::get_epoch());
        token = static_cast<void *>(&(elem));
        return status::OK;
      }
    }
    return status::WARN_MAX_SESSIONS;
  }

  static std::array<session_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> &get_thread_info_table() {
    return kThreadInfoTable;
  }

  /**
   * @pre global epoch is not yet functional because it assigns 0 to begin_epoch as the initial value.
   */
  static void init() {
    for (auto itr = kThreadInfoTable.begin(); itr != kThreadInfoTable.end(); ++itr) { // NOLINT
      itr->set_begin_epoch(0);
      itr->set_gc_container(static_cast<size_t>(std::distance(kThreadInfoTable.begin(), itr)));
      itr->set_running(false);
      /**
       * If the fin function is called with the session opened before, it will deadlock in the assign session.
       * To prevent this, be sure to unlock the lock here.
       */
      itr->try_lock_epoch();
      itr->unlock_epoch();
    }
  }

  /**
   * @details When @a token points to an invalid memory location, an error occurs if @a token is referenced.
   * To avoid this, it scans the table.
   * @tparam interior_node
   * @tparam border_node
   * @param token
   * @return
   */
  template<class interior_node, class border_node>
  static status leave_session(Token token) {
    for (auto &&elem : kThreadInfoTable) {
      if (token == static_cast<void *>(&(elem))) {
        elem.gc<interior_node, border_node>();
        elem.set_running(false);
        elem.set_begin_epoch(0);
        elem.unlock_epoch();
        return status::OK;
      }
    }
    return status::WARN_INVALID_TOKEN;
  }

private:
  static inline std::array<session_info, YAKUSHIMA_MAX_PARALLEL_SESSIONS> kThreadInfoTable; // NOLINT
};

} // namespace yakushima