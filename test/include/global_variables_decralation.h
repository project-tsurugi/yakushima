/**
 * @file global_variables_decralation.h
 */

#pragma once

#include "kvs.h"

namespace yakushima {

std::atomic<base_node *> masstree_kvs::root_;

} // namespace yakushima
