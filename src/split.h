#pragma once

#include "border_node.h"
#include "interior_node.h"
#include "link_or_value.h"

namespace yakushima {

void split(border_node* bnode, std::string_view key_view,
        void *value_ptr, link_or_value::value_length_type value_length,
        link_or_value::value_align_type value_align) {
  return;
}

} // namespace yakushima
