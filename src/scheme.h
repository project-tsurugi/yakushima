/**
 * @file scheme.h
 * @brief define status code.
 */

#pragma once

namespace yakushima {

enum class status : std::int32_t {
  /**
   * @brief Warning of mistaking usage.
   * @details
   */
  WARN_BAD_USAGE,
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
  /**
   * @brief root is not both interior and border.
   */
   ERR_UNKNOWN_ROOT,
};

} // namespace yakushima