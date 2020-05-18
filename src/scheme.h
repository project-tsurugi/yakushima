/**
 * @file scheme.h
 * @brief define status code.
 */

#pragma once

namespace yakushima {

enum class status : std::int32_t {
  /**
   * @brief Warning of mistaking usage.
   */
  WARN_BAD_USAGE,
  /**
   * @brief Warning
   * @details (insert/put/get/delete) No corresponding value in masstree.
   */
  WARN_NOT_FOUND,
  /**
   * @brief Warning
   * @details This status tells that find_border must retry from root of all tree.
   */
  WARN_ROOT_DELETED,
  /**
   * @brief Warning
   * @details Masstree originally has a unique key constraint.
   * todo (optional): This constraint is removed.
   */
  WARN_UNIQUE_RESTRICTION,
  /**
   * @biref success status
   */
  OK,
  /**
   * @brief (destroy) It destroys xxx.
   */
  OK_DESTROY_BORDER,
  /**
   * @brief (destroy) It destroys xxx.
   */
  OK_DESTROY_INTERIOR,
  /**
   * @brief root is not both interior and border.
   */
  ERR_UNKNOWN_ROOT,
};

} // namespace yakushima