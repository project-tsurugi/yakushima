/**
 * @file mt_border_node.h
 */

#pragma once

#include <cstdint>
#include <iostream>

#include "atomic_wrapper.h"
#include "link_or_value.h"
#include "permutation.h"

namespace yakushima {

class border_node final : public base_node {
public:
  using key_length_type = std::uint8_t;
  using n_removed_type = std::uint8_t;

  border_node() = default;

  ~border_node() = default;

  /**
   * @brief release all heap objects and clean up.
   */
  status destroy() final {
    for (auto i = 0; i < permutation_.get_cnk(); ++i) {
      lv_[i].destroy();
    }
    delete this;
    return status::OK_DESTROY_BORDER;
  }

  [[nodiscard]] link_or_value *get_lv_at(std::size_t index) {
    return &lv_[index];
  }

  /**
   * @attention This function must not be called with locking of this node. Because this function executes
   * get_stable_version and it waits own (lock-holder) infinitely.
   * @param[in] key_slice
   * @param[out] stable_v  the stable version which is at atomically fetching lv.
   * @return
   */
  [[nodiscard]] link_or_value *get_lv_of(base_node::key_slice_type key_slice, node_version64_body &stable_v) {
    node_version64_body v = get_stable_version();
    for (;;) {
      /**
       * It loads cnk atomically by get_cnk func.
       */
      std::size_t cnk = permutation_.get_cnk();
      link_or_value *ret_lv{nullptr};
      for (std::size_t i = 0; i < cnk; ++i) {
        if (key_slice == get_key_slice_at(i)) {
          ret_lv = get_lv_at(i);
          break;
        }
      }
      node_version64_body v_check = get_stable_version();
      if (v == v_check) {
        stable_v = v;
        return ret_lv;
      } else {
        v = v_check;
      }
    }
  }

  /**
   * @param key_slice
   * @details This function extracts lv without lock (double checking stable version).
   * @return link_or_value*
   * @return nullptr
   */
  [[nodiscard]] link_or_value *get_lv_of_without_lock(base_node::key_slice_type key_slice) {
    std::size_t cnk = permutation_.get_cnk();
    for (std::size_t i = 0; i < cnk; ++i) {
      if (key_slice == get_key_slice_at(i)) {
        return get_lv_at(i);
      }
    }
    return nullptr;
  }

  [[nodiscard]] uint8_t *get_key_length() {
    return key_length_;
  }

  void init_border() {
    init_base();
    set_version_border(true);
    n_removed_ = 0;
    for (std::size_t i = 0; i < key_slice_length; ++i) {
      key_length_[i] = 0;
      lv_[i].init_lv();
    }
    permutation_.init();
    next_ = nullptr;
    prev_.store(nullptr, std::memory_order_relaxed);
  }

  /**
   * @pre This function is called by put function.
   * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
   * @pre This function can not be called for updating existing nodes.
   * @pre If this function is used for node creation, link after set because
   * set function does not execute lock function.
   * @details This function inits border node by using arguments.
   * @param key_view
   * @param value_ptr
   * @param[in] root is the root node of the layer.
   */
  template<class ValueType>
  void init_border(std::string_view key_view,
                   ValueType *value_ptr,
                   bool root,
                   std::size_t arg_value_length = sizeof(ValueType),
                   std::size_t value_align = alignof(ValueType)) {
    init_border();
    set_version_root(root);
    next_ = nullptr;
    prev_ = nullptr;
    insert_lv_at(0, key_view, value_ptr, arg_value_length, value_align);
  }

  /**
   * @pre It already locked this node.
   */
  template<class ValueType>
  void insert_lv(std::string_view key_view,
                 ValueType *value_ptr,
                 std::size_t arg_value_length,
                 std::size_t value_align) {
    set_version_inserting(true);
    std::size_t cnk = permutation_.get_cnk();
    if (cnk == key_slice_length) {
      /**
       * It needs splitting
       */
    } else {
      /**
       * Insert into this nodes.
       */
       insert_lv_at(cnk, key_view, value_ptr, arg_value_length, value_align);
    }
  }

  /**
   * @pre It already locked this node.
   */
  template<class ValueType>
  void insert_lv_at(std::size_t index,
                    std::string_view key_view,
                    ValueType *value_ptr,
                    std::size_t arg_value_length,
                    std::size_t value_align) {
    base_node::key_slice_type key_slice;
    if (key_view.size() > sizeof(base_node::key_slice_type)) {
      /**
       * Create multiple border nodes.
       */
      memcpy(&key_slice, key_view.data(), sizeof(base_node::key_slice_type));
      set_key_slice(index, key_slice);
      set_key_length(index, sizeof(base_node::key_slice_type));
      permutation_.inc_key_num();
      permutation_.rearrange(get_key_slice(), get_key_length());
      border_node *next_layer_border = new border_node();
      set_lv_next_layer(index, next_layer_border);
      key_view.remove_prefix(sizeof(key_slice_type));
      /**
       * @attention next_layer_border is the root of next layer.
       */
      static_cast<border_node *>(next_layer_border)->init_border(key_view, value_ptr, true, arg_value_length, value_align);
    } else {
      memcpy(&key_slice, key_view.data(), key_view.size());
      set_key_slice(index, key_slice);
      set_key_length(index, key_view.size());
      permutation_.inc_key_num();
      permutation_.rearrange(get_key_slice(), get_key_length());
      set_lv_value(index, value_ptr, arg_value_length, value_align);
    }
  }

  void set_key_length(std::size_t index, uint8_t length) {
    key_length_[index] = length;
  }

private:

  template<class ValueType>
  void set_lv_value(std::size_t index,
                    ValueType *value,
                    std::size_t arg_value_length = sizeof(ValueType),
                    std::size_t value_align = alignof(ValueType)) {
    lv_[index].set_value(value, arg_value_length, value_align);
  }

  void set_lv_next_layer(std::size_t index, base_node *next_layer) {
    lv_[index].set_next_layer(next_layer);
  }

  // first member of base_node is aligned along with cache line size.
  /**
   * @attention This variable is read/written concurrently.
   * tanabe : I don't know this variable's value. There is a  no purpose (details) in the original paper.
   */
  n_removed_type n_removed_{};
  /**
   * @attention This variable is read/written concurrently.
   * @details This is used for distinguishing the identity of link or value and same slices.
   * For example,
   * key 1 : \0, key 2 : \0\0, ... , key 8 : \0\0\0\0\0\0\0\0.
   * These keys have same key_slices (0) but different key_length.
   * If the length is more than 8, the lv points out to next layer.
   */
  key_length_type key_length_[key_slice_length]{};
  /**
   * @attention This variable is read/written concurrently.
   */
  permutation permutation_{};
  /**
   * @attention This variable is read/written concurrently.
   */
  link_or_value lv_[key_slice_length]{};
  /**
   * @attention This variable is read/written concurrently.
   */
  border_node *next_{nullptr};
  /**
   * @attention This is protected by its previous sibling's lock.
   */
  std::atomic<border_node *> prev_{nullptr};
  /**
   * @attention This variable is read concurrently.
   * This variable is updated only at initialization.
   * tanabe : I don't know this variable's value. There is a  no purpose (details) in the original paper.
   */
  std::uint64_t key_suffix_[key_slice_length]{};
};
} // namespace yakushima

