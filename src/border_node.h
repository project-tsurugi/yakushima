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

class border_node final : public base_node {
public:
  border_node() = default;

  ~border_node() = default;

  /**
   * @details This may be called at split function.
   * It creates new interior node as parents of this border_node and @a higher_border node.
   * After that, it inserts based on @a key_view, @a value_ptr, ... (args).
   * @param higher_border This is a higher border_node as result of split for this node.
   * @param key_view
   * @param value_ptr
   * @param value_length
   * @param value_align
   * @param lock_list This is unused because the border nodes is not-full as result of split.
   * The insert_lv function needs lock_list as an argument, so it is passed in spite of not using.
   */
  void create_interior_parents_and_insert(border_node *higher_border,
                                          std::string_view key_view, bool next_layer, void *value_ptr,
                                          value_length_type value_length,
                                          value_align_type value_align, std::vector<node_version64 *> &lock_list) {
    /**
     * create a new interior node p with children n, n'
     */
    interior_node *ni = new interior_node();
    ni->init_interior();
    ni->set_version_root(true);
    ni->lock();
    lock_list.emplace_back(ni->get_version_ptr());
    /**
     * process base node members
     */
    ni->set_key(0, higher_border->get_key_slice_at(0), higher_border->get_key_length_at(0));
    /**
     * process interior node members
     */
    ni->n_keys_increment();
    ni->set_child_at(0, dynamic_cast<base_node *>(this));
    ni->set_child_at(1, dynamic_cast<base_node *>(higher_border));
    /**
     * The insert process we wanted to do before we split.
     * key_slice must be initialized to 0.
     */
    key_slice_type key_slice(0);
    if (key_view.size() > sizeof(key_slice_type)) {
      memcpy(&key_slice, key_view.data(), sizeof(key_slice_type));
    } else {
      memcpy(&key_slice, key_view.data(), key_view.size());
    }
    key_slice_type lowest_key_of_nb = higher_border->get_key_slice_at(0);
    key_length_type lowest_key_length_of_nb = higher_border->get_key_length_at(0);
    int memcmp_result = memcmp(&key_slice, &lowest_key_of_nb, sizeof(key_slice_type));
    if (memcmp_result < 0
        || (memcmp_result == 0 && key_view.size() < lowest_key_length_of_nb)) {
      /**
       * insert to lower border node.
       * @attention lock_list will not be added new lock.
       */
      insert_lv(key_view, next_layer, value_ptr, value_length, value_align, lock_list);
    } else if (memcmp_result > 0
               || (memcmp_result == 0 && key_view.size() > lowest_key_length_of_nb)) {
      /**
       * insert to higher border node.
       * @attention lock_list will not be added new lock.
       */
      higher_border->insert_lv(key_view, next_layer, value_ptr, value_length, value_align, lock_list);
    } else {
      /**
       * It did not have a matching key, so it ran inert_lv.
       * There should be no matching keys even if it splited this border.
       */
      std::cerr << __FILE__ << " : " << __LINE__ << " : " << "split fatal error" << std::endl;
      std::abort();
    }
    /**
     * release interior parent to global.
     */
    set_parent(dynamic_cast<base_node *>(ni));
    higher_border->set_parent(dynamic_cast<base_node *>(ni));
    set_root(dynamic_cast<base_node *>(ni));
  }

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
  [[nodiscard]] link_or_value *get_lv_of(key_slice_type key_slice, node_version64_body &stable_v) {
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
  [[nodiscard]] link_or_value *get_lv_of_without_lock(key_slice_type key_slice) {
    std::size_t cnk = permutation_.get_cnk();
    for (std::size_t i = 0; i < cnk; ++i) {
      if (key_slice == get_key_slice_at(i)) {
        return get_lv_at(i);
      }
    }
    return nullptr;
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
    init_border_member_range(0, key_slice_length - 1);
    set_version_root(root);
    set_version_border(true);
    next_ = nullptr;
    prev_ = nullptr;
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
   * @details This function is also called when creating a new layer when 8 bytes-key collides at a border node.
   * At that time, the original value is moved to the new layer.
   * This function does not use a template declaration because its pointer is retrieved with void *.
   * @param[out] lock_list Hold the lock so that the caller can release the lock from below.
   */
  void insert_lv(std::string_view key_view,
                 bool next_layer,
                 void *value_ptr,
                 value_length_type arg_value_length,
                 value_align_type value_align,
                 std::vector<node_version64 *> &lock_list) {
    set_version_inserting(true);
    std::size_t cnk = permutation_.get_cnk();
    if (cnk == key_slice_length) {
      /**
       * It needs splitting
       */
      split(key_view, next_layer, value_ptr, arg_value_length, value_align, lock_list);
    } else {
      /**
       * Insert into this nodes.
       */
      insert_lv_at(cnk, key_view, next_layer, value_ptr, arg_value_length, value_align);
    }
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
      memcpy(&key_slice, key_view.data(), key_view.size());
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

private:

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

  /**
   * @pre It already locked this node.
   * @details border node split.
   * @param[in] key_view
   * @param[in] value_ptr
   * @param[in] value_length
   * @param[in] value_align
   * @param[out] lock_list Hold the lock so that the caller can release the lock from below.
   */
  void split(std::string_view key_view,
             bool next_layer,
             void *value_ptr,
             value_length_type value_length,
             value_align_type value_align,
             std::vector<node_version64 *> &lock_list) {
    border_node *new_border = new border_node();
    new_border->init_border();
    set_next(new_border);
    new_border->set_prev(this);
    set_version_root(false);
    set_version_splitting(true);
    /**
     * new border is initially locked
     */
    new_border->set_version(get_version());
    lock_list.emplace_back(new_border->get_version_ptr());
    /**
     * split keys among n and n'
     */
    constexpr std::size_t key_slice_index = 0;
    constexpr std::size_t key_length_index = 1;
    constexpr std::size_t key_pos = 2;
    std::vector<std::tuple<key_slice_type, key_length_type, std::uint8_t>> vec;
    std::uint8_t cnk = permutation_.get_cnk();
    vec.reserve(cnk);
    key_slice_type *key_slice_array = get_key_slice();
    key_length_type *key_length_array = get_key_length();
    for (std::uint8_t i = 0; i < cnk; ++i) {
      vec.emplace_back(key_slice_array[i], key_length_array[i], i);
    }
    std::sort(vec.begin(), vec.end());

    /**
     * split
     */
    std::size_t index_ctr(0);
    std::vector<std::size_t> shift_pos;
    for (auto itr = vec.begin() + (key_slice_length / 2); itr != vec.end(); ++itr) {
      /**
       * move base_node members to new nodes
       */
      new_border->set_key_slice_at(index_ctr, std::get<key_slice_index>(*itr));
      new_border->set_key_length_at(index_ctr, std::get<key_length_index>(*itr));
      new_border->set_lv(index_ctr, get_lv_at(index_ctr));
      if (std::get<key_pos>(*itr) < (key_slice_length / 2)) {
        shift_pos.emplace_back(std::get<key_pos>(*itr));
      }
      ++index_ctr;
    }
    /**
     * fix member positions of old border_node.
     */
    std::sort(shift_pos.begin(), shift_pos.end());
    std::size_t shifted_ctr(0);
    for (auto itr = shift_pos.begin(); itr != shift_pos.end(); ++itr) {
      shift_left_base_member(*itr + 1 - shifted_ctr, 1);
      shift_left_border_member(*itr + 1 - shifted_ctr, 1);
      ++shifted_ctr;
    }
    /**
     * maintenance about empty parts due to new border.
     */
    init_base_member_range(key_slice_length / 2, key_slice_length - 1);
    init_border_member_range(key_slice_length / 2, key_slice_length - 1);
    /**
     * fix permutations
     */
    permutation_.set_cnk(key_slice_length / 2);
    permutation_.rearrange(get_key_slice(), get_key_length());
    new_border->permutation_.set_cnk(key_slice_length - key_slice_length / 2);
    new_border->permutation_.rearrange(new_border->get_key_slice(), new_border->get_key_length());

    base_node *p = lock_parent();
    if (p == nullptr) {
      /**
       * create interior as parents and insert k.
       */
      create_interior_parents_and_insert(new_border, key_view, next_layer, value_ptr, value_length, value_align,
                                         lock_list);
      return;
    }
    /**
     * p exists.
     */
    lock_list.emplace_back(p->get_version_ptr());
    if (p->get_version_border()) {
      /**
       * parent is border node.
       * The old border node which is before this split was root of the some layer.
       * So it creates new interior nodes in the layer and insert its interior pointer
       * to the (parent) border node.
       */
      border_node *pb = dynamic_cast<border_node *>(p);
      pb->set_version_inserting(true);
      create_interior_parents_and_insert(new_border, key_view, next_layer, value_ptr, value_length,
                                         value_align, lock_list);
      interior_node *pi = dynamic_cast<interior_node *>(get_parent());
      if (pb->permutation_.get_cnk() == base_node::key_slice_length) {
        /**
         * border full case, it splits and inserts.
         */
        split(key_view, true, pi, value_length, value_align, lock_list);
        return;
      }
      /**
       * border not-full case, it inserts.
       */
      pb->insert_lv(std::string_view
                            {reinterpret_cast<char *>(pi->get_key_slice_at(0)), pi->get_key_length_at(0)},
                    true, pi, 0, 0, lock_list);
      return;
    }
    /**
     * parent is interior node.
     */
    interior_node *pi = dynamic_cast<interior_node *>(p);
    if (pi->get_n_keys() == base_node::key_slice_length) {
      /**
       * interior full case, it splits and inserts.
       */
       pi->split(new_border, lock_list);
      return;
    }
    /**
     * interior not-full case, it inserts.
     */
    pi->set_version_inserting(true);
    pi->insert(new_border);
    return;
  }

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
   * @attention This variable is read/written concurrently.
   */
  border_node *next_{nullptr};
  /**
   * @attention This is protected by its previous sibling's lock.
   */
  std::atomic<border_node *> prev_{nullptr};
};
} // namespace yakushima

