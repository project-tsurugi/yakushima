/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>
#include <iostream>

#include "interior_node.h"
#include "link_or_value.h"
#include "permutation.h"

// forward declaration
struct interior_node;

namespace yakushima {

template<class ValueType>
class border_node : private base_node {
public:
  border_node()
          : base_node{},
            nremoved_{},
            key_length_{},
            permutation_{},
            lv_{},
            next_{nullptr},
            prev_{nullptr},
            key_suffix_{} {}

  /**
   * @brief release all heap objects and clean up.
   */
  void destroy() override {
    /**
     * todo : Call destroy of all children.
     */
  }

  [[nodiscard]] uint8_t *get_key_length() {
    return key_length_;
  }

  /**
   * @pre This function is called by put function when the tree is empty.
   * @param key
   * @param value
   */
  void set_as_root(std::string key, ValueType *value, std::size_t value_length) {
    set_version_root(true);
    set_version_leaf(true);
    uint64_t key_slice;
    // firstly, it assumes key length less than 8.
    // todo : adapt that key is larger than 8.
    memcpy(&key_slice, key.data(), key.size());
    set_key_slice(0, key_slice);
    set_key_length(0, key.size());
    set_lv(0, value, value_length);
    permutation_.inc_key_num();
    permutation_.rearrange(get_key_slice(), get_key_length());
    next_ = nullptr;
    prev_ = nullptr;
    key_suffix_ = key;
  }

  void set_key_length(std::size_t index, uint8_t length) {
    key_length_[index] = length;
  }

private:

  void set_lv(std::size_t index, ValueType *value, std::size_t value_length) {
    lv_[index].set_value(value, value_length);
  }

  // first member of base_node is aligned along with cache line size.
  uint8_t nremoved_{};
  uint8_t key_length_[node_fanout]{};
  permutation permutation_{};
  link_or_value<ValueType> lv_[node_fanout]{};
  border_node *next_{nullptr};
  /**
   * @attention This is protected by its previous sibling's lock.
   */
  border_node *prev_{nullptr};
  std::string key_suffix_{};
};
} // namespace yakushima

