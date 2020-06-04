/**
 * @file interior_node.h
 */

#pragma once

#include <string.h>

#include <cstdint>
#include <cstring>
#include <iostream>

#include "atomic_wrapper.h"
#include "base_node.h"
#include "thread_info.h"

#include "../test/include/debug.hh"

using std::cout, std::endl;

namespace yakushima {

class interior_node final : public base_node {
public:
  /**
   * @details The structure is "ptr, key, ptr, key, ..., ptr".
   * So the child_length is key_slice_length plus 1.
   */
  static constexpr std::size_t child_length = base_node::key_slice_length + 1;
  using n_keys_body_type = std::uint8_t;
  using n_keys_type = std::atomic<n_keys_body_type>;

  interior_node() = default;

  ~interior_node() = default;

  /**
   * @pre There is a child which is the same to @a child.
   * @post If the number of children is 1, It asks caller to make the child to root and delete this node.
   * Therefore, it place the-only-one child to position 0.
   * @details Delete operation on the element matching @a child.
   * @param[in] token
   * @param[in] child
   */
  void delete_of(Token token, base_node *child) final {
    std::size_t n_key = get_n_keys();
    for (std::size_t i = 0; i <= n_key; ++i) {
      if (get_child_at(i) == child) {
        if (n_key == 1) {
          base_node *pn = lock_parent();
          if (pn == nullptr) {
            /**
             * todo : consider deeply whetehr it is correct.
             */
            get_child_at(!i)->set_parent(nullptr);
            base_node::set_root(get_child_at(!i)); // i == 0 or 1
            base_node::get_root()->atomic_set_version_root(true);
          } else {
            pn->delete_of(token, this);
            pn->unlock();
          }
          set_version_deleted(true);
          reinterpret_cast<thread_info *>(token)->move_node_to_gc_container(this);
        } else { // n_key > 1
          if (i == 0) { // leftmost points
            shift_left_base_member(2, 2);
            shift_left_children(1, 1);
            set_key(n_key - 1, 0, 0);
            set_key(n_key - 2, 0, 0);
          } else if (i == n_key) { // rightmost points
            // no unique process
            set_key(n_key - 1, 0, 0);
          } else { // middle points
            shift_left_base_member(i, 1);
            shift_left_children(i + 1, 1);
            set_key(n_key - 1, 0, 0);
          }
          set_child_at(n_key, nullptr);
        }
        set_version_vdelete(get_version_vdelete() + 1);
        n_keys_decrement();
        return;
      }
    }
    std::cerr << __FILE__ << " : " << __LINE__ << " : precondition failure." << endl;
    std::abort();
  }

  /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   */
  status destroy() final {
    for (auto i = 0; i < n_keys_ + 1; ++i) {
      get_child_at(i)->destroy();
    }
    delete this;
    return status::OK_DESTROY_INTERIOR;
  }

  /**
   * @details display function for analysis and debug.
   */
  void display() final {
    display_base();
    cout << "interior_node::display" << endl;
    cout << "nkeys_ : " << std::to_string(get_n_keys()) << endl;
    for (std::size_t i = 0; i <= get_n_keys(); ++i) {
      cout << "child : " << i << " : " << get_child_at(i) << endl;
    }
  }

  [[nodiscard]] n_keys_body_type get_n_keys() {
    return n_keys_.load(std::memory_order_acquire);
  }

  [[nodiscard]] base_node* get_child_at(std::size_t index) {
    return loadAcquireN(children[index]);
  }

  base_node *get_child_of(key_slice_type key_slice, key_length_type key_length) {
    node_version64_body v = get_stable_version();
    std::tuple<key_slice_type, key_length_type> visitor{key_slice, key_length};
    for (;;) {
      n_keys_body_type n_key = get_n_keys();
      base_node *ret_child{nullptr};
      for (auto i = 0; i < n_key; ++i) {
        /**
         * It loads key_slice atomically by get_key_slice_at func.
         */
        std::tuple<key_slice_type, key_length_type> resident{get_key_slice_at(i), get_key_length_at(i)};
        if (visitor < resident) {
          /**
           * The key_slice must be left direction of the index.
           */
          ret_child = get_child_at(i);
          break;
        } else {
          /**
           * The key_slice must be right direction of the index.
           */
          if (i == n_key - 1) {
            ret_child = get_child_at(i + 1);
            break;
          }
        }
      }
      node_version64_body check = get_stable_version();
      if (v == check) return ret_child;
      else v = check;
    }
  }

  void init_interior() {
    init_base();
    set_version_border(false);
    for (std::size_t i = 0; i < child_length; ++i) {
      set_child_at(i, nullptr);
    }
    set_n_keys(0);
  }

  /**
   * @pre It already acquired lock of this node.
   * @pre This interior node is not full.
   * @details insert @a child and fix @a children.
   * @param child new inserted child.
   */
  void insert(base_node *child) {
    std::tuple<key_slice_type, key_length_type>
            visitor{child->get_key_slice_at(0), child->get_key_length_at(0)};
    n_keys_body_type n_key = get_n_keys();
    for (auto i = 0; i < n_key; ++i) {
      std::tuple<key_slice_type, key_length_type>
              resident{get_key_slice_at(i), get_key_length_at(i)};
      constexpr std::size_t visitor_slice = 0;
      constexpr std::size_t visitor_slice_length = 1;
      if (visitor < resident) {
        shift_right_base_member(i, 1);
        set_key(i, std::get<visitor_slice>(visitor), std::get<visitor_slice_length>(visitor));
        shift_right_children(i + 1, 1);
        set_child_at(i + 1, child);
        n_keys_increment();
        return;
      }
    }
    // insert to rightmost points
    set_key(n_key, child->get_key_slice_at(0), child->get_key_length_at(0));
    set_child_at(n_key + 1, child);
    child->set_parent(this);
    n_keys_increment();
    return;
  }

  void move_children_to_interior_range(interior_node *right_interior, std::size_t start) {
    for (auto i = start; i < child_length; ++i) {
      right_interior->set_child_at(i - start, get_child_at(i));
      /**
       * right interiror is new parent of get_child_at(i).
       */
      get_child_at(i)->set_parent(dynamic_cast<base_node*>(right_interior));
      set_child_at(i, nullptr);
    }
  }

  void set_child_at(std::size_t index, base_node *new_child) {
    storeReleaseN(children[index], new_child);
  }

  void set_n_keys(n_keys_body_type new_n_key) {
    n_keys_.store(new_n_key, std::memory_order_release);
  }

  /**
   * @pre It already acquired lock of this node.
   * @param start_pos
   * @param shift_size
   */
  void shift_left_children(std::size_t start_pos, std::size_t shift_size) {
    for (std::size_t i = start_pos; i < child_length; ++i) {
      set_child_at(i - shift_size, get_child_at(i));
    }
  }

  /**
   * @pre It already acquired lock of this node.
   * @param start_pos
   * @param shift_size
   */
  void shift_right_children(std::size_t start_pos, std::size_t shift_size) {
    memmove(reinterpret_cast<void *>(get_child_at(start_pos + shift_size)),
            reinterpret_cast<void *>(get_child_at(start_pos)),
            sizeof(base_node *) * (child_length - start_pos - shift_size));
  }

  void n_keys_decrement() {
    n_keys_.fetch_sub(1);
  }

  void n_keys_increment() {
    n_keys_.fetch_add(1);
  }

private:
  /**
   * first member of base_node is aligned along with cache line size.
   */

  /**
   * @attention This variable is read/written concurrently.
   */
  base_node *children[child_length]{};
  /**
   * @attention This variable is read/written concurrently.
   */
  n_keys_type n_keys_{};
};

} // namespace yakushima
