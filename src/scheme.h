/**
 * @file scheme.h
 * @brief define status code.
 */

#pragma once

namespace yakushima {

/**
 * @brief Session token
 */
using Token = void *;

enum class status : std::int32_t {
  /**
   * @brief Warning of mistaking usage.
   */
  WARN_BAD_USAGE,
  /**
   * @brief warning
   */
  WARN_INVALID_TOKEN,
  /**
   * @brief Warning
   * @details The find_border function tells that it must retry from root of all tree.
   */
  WARN_RETRY_FROM_ROOT_OF_ALL,
  /**
   * @brief Warning
   * @details (assign_session) The maximum number of sessions is already up and running.
   */
  WARN_MAX_SESSIONS,
  /**
   * @brief Warning
   * @details Masstree originally has a unique key constraint.
   * todo (optional): This constraint is removed.
   */
  WARN_UNIQUE_RESTRICTION,
  /**
   * @brief success status
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
   * @brief Warning
   * @details (get/delete) No corresponding value in this storage engine.
   */
  OK_NOT_FOUND,
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