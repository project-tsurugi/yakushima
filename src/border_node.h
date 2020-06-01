/**
 * @file mt_border_node.h
 */

#pragma once

#include <string.h>

#include <cstdint>
#include <iostream>

#include "atomic_wrapper.h"
#include "interior_node.h"
#include "link_or_value.h"
#include "permutation.h"

namespace yakushima {

using std::cout;
using std::endl;

class border_node final : public base_node {
public:
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

  /**
   * @pre This border node was already locked by caller.
   * @details delete value corresponding to @a key_slice and @a key_length
   * @param key_slice The key slice of key-value.
   * @param key_length The @key_slice length.
   */
  void delete_of([[maybe_unused]]key_slice_type key_slice, [[maybe_unused]]key_length_type key_slice_length) {
  }

  /**
   * @details display function for analysis and debug.
   */
  void display() final {
    display_base();
    cout << "border_node::display" << endl;
    permutation_.display();
    for (std::size_t i = 0; i < get_permutation_cnk(); ++i) {
      lv_[i].display();
    }
    cout << "next : " << get_next() << endl;
  }

  [[nodiscard]] std::uint8_t get_permutation_cnk() {
    return permutation_.get_cnk();
  }

  [[nodiscard]] link_or_value *get_lv() {
    return lv_;
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
  [[nodiscard]] link_or_value *get_lv_of(key_slice_type key_slice, key_length_type key_length, node_version64_body &stable_v) {
    node_version64_body v = get_stable_version();
    for (;;) {
      /**
       * It loads cnk atomically by get_cnk func.
       */
      std::size_t cnk = permutation_.get_cnk();
      link_or_value *ret_lv{nullptr};
      for (std::size_t i = 0; i < cnk; ++i) {
        if (key_slice == get_key_slice_at(i) && key_length == get_key_length_at(i)) {
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
  [[nodiscard]] link_or_value *get_lv_of_without_lock(key_slice_type key_slice, key_length_type key_length) {
    std::size_t cnk = permutation_.get_cnk();
    for (std::size_t i = 0; i < cnk; ++i) {
      if (key_slice == get_key_slice_at(i) && key_length == get_key_length_at(i)) {
        return get_lv_at(i);
      }
    }
    return nullptr;
  }

  border_node *get_next() {
    return loadAcquireN(next_);
  }

  border_node *get_prev() {
    return prev_.load(std::memory_order_acquire);
  }

  void init_border() {
    init_base();
    init_border_member_range(0, key_slice_length - 1);
    set_version_border(true);
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
                   value_length_type arg_value_length = sizeof(ValueType),
                   std::size_t value_align = alignof(ValueType)) {
    init_border();
    set_version_root(root);
    set_version_border(true);
    insert_lv_at(0, key_view, false, value_ptr, arg_value_length, value_align);
  }

  void init_border_member_range(std::size_t start, std::size_t end) {
    for (auto i = start; i <= end; ++i) {
      init_lv_at(i);
    }
  }

  void init_lv_at(std::size_t index) {
    lv_[index].init_lv();
  }

  /**
   * @pre It already locked this node.
   * @param[in] next_layer If it is true, value_ptr points to next_layer.
   */
  void insert_lv_at(std::size_t index,
                    std::string_view key_view,
                    bool next_layer,
                    void *value_ptr,
                    value_length_type arg_value_length,
                    value_align_type value_align) {
    /**
     * @attention key_slice must be initialized to 0.
     * If key_view.size() is smaller than sizeof(key_slice_type),
     * it is not possible to update the whole key_slice object with memcpy.
     * It is possible that undefined values may remain from initialization.
     */
    key_slice_type key_slice(0);
    if (key_view.size() > sizeof(key_slice_type)) {
      /**
       * Create multiple border nodes.
       */
      memcpy(&key_slice, key_view.data(), sizeof(key_slice_type));
      set_key_slice_at(index, key_slice);
      set_key_length_at(index, sizeof(key_slice_type));
      permutation_.inc_key_num();
      permutation_.rearrange(get_key_slice(), get_key_length());
      border_node *next_layer_border = new border_node();
      set_lv_next_layer(index, next_layer_border);
      key_view.remove_prefix(sizeof(key_slice_type));
      /**
       * @attention next_layer_border is the root of next layer.
       */
      static_cast<border_node *>(next_layer_border)->init_border(key_view, value_ptr, true, arg_value_length,
                                                                 value_align);
    } else {
      if (key_view.size() > 0) {
        memcpy(&key_slice, key_view.data(), key_view.size());
      } else {
        key_slice = 0;
      }
      set_key_slice_at(index, key_slice);
      set_key_length_at(index, key_view.size());
      permutation_.inc_key_num();
      permutation_.rearrange(get_key_slice(), get_key_length());
      if (next_layer) {
        set_lv_next_layer(index, reinterpret_cast<base_node *>(value_ptr));
      }
      set_lv_value(index, value_ptr, arg_value_length, value_align);
    }
  }

  void set_permutation_cnk(std::uint8_t ncnk) {
    permutation_.set_cnk(ncnk);
  }

  void set_permutation_rearrange() {
    permutation_.rearrange(get_key_slice(), get_key_length());
  }

  /**
   * @pre This is called by split process.
   * @param index
   * @param nlv
   */
  void set_lv(std::size_t index, link_or_value *nlv) {
    lv_[index].set(nlv);
  }

  void set_lv_value(std::size_t index,
                    void *value,
                    value_length_type arg_value_length,
                    value_align_type value_align) {
    lv_[index].set_value(value, arg_value_length, value_align);
  }

  void set_lv_next_layer(std::size_t index, base_node *next_layer) {
    lv_[index].set_next_layer(next_layer);
  }

  void set_next(border_node *nnext) {
    storeReleaseN(next_, nnext);
  }

  void set_prev(border_node *nprev) {
    prev_.store(nprev, std::memory_order_release);
  }

  void shift_left_border_member(std::size_t start_pos, std::size_t shift_size) {
    memmove(get_lv_at(start_pos - shift_size), get_lv_at(start_pos),
            sizeof(link_or_value) * (key_slice_length - start_pos));
  }

private:
  // first member of base_node is aligned along with cache line size.
  /**
   * @attention This variable is read/written concurrently.
   */
  permutation permutation_{};
  /**
   * @attention This variable is read/written concurrently.
   */
  link_or_value lv_[key_slice_length]{};
  /**
   * @attention This is protected by its previous sibling's lock.
   */
  std::atomic<border_node *> prev_{nullptr};
  /**
   * @attention This variable is read/written concurrently.
   */
  border_node *next_{nullptr};
};
} // namespace yakushima

