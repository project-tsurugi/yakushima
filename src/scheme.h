/**
 * @file scheme.h
 * @brief define status code.
 */

#pragma once

namespace yakushima {

enum class status : std::int32_t {
  /**
   * @brief Warning
   * @details
   * (insert/put/get/delete) No corresponding value in masstree.
   */
  WARN_NOT_FOUND,
  /**
   * @biref success status
   */
  OK,
};

} // namespace yakushima