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
   * (create_empty_border_node_as_root()) Root is not nullptr.
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
};

} // namespace yakushima