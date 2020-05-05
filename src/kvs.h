/**
 * @file mt_kvs.h
 * @brief This file defines the body of masstree.
 */

#pragma once

#include "border_node.h"

namespace yakushima {

template <class ValueType>
class masstree_kvs {
public:
  static status remove(std::string key) {}

  static status get(std::string key) {}

  static init_kvs() {}

  static status insert(std::string key, ValueType value) {}

  static status put(std::string key, ValueType value) {}

private:
  /**
   * @details
   * Todo : It will be container to be able to switch database.
   */
  static std::atomic<base_node<ValueType>*> root_;
};

/**
 * @details
 * It should declare real object in 1 source files due to single static global object.
 */
extern template <ValueType>
masstree_kvs<ValueType>::root_;

} // namespace yakushima
