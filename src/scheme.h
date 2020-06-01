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
   * @details (insert/put/get/delete) No corresponding value in this storage engine.
   */
  WARN_NOT_FOUND,
  /**
   * @brief Warning
   * @details The find_border function tells that it must retry from root of all tree.
   */
  WARN_RETRY_FROM_ROOT_OF_ALL,
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
   * @brief (destroy) It destroys existing all trees.
   */
  OK_DESTROY_ALL,
  /**
   * @brief (destroy) It destroys xxx.
   */
  OK_DESTROY_BORDER,
  /**
   * @brief (destroy) It destroys xxx.
   */
  OK_DESTROY_INTERIOR,
  /**
   * @brief
   * (destroy) Root is nullptr and it could not destroy.
   * (remove) No existing tree.
   */
  OK_ROOT_IS_NULL,
  /**
   * @brief root is not both interior and border.
   */
  ERR_UNKNOWN_ROOT,
};

} // namespace yakushima