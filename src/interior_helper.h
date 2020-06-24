/**
 * @file interior_helper.h
 * @details Declare functions that could not be member functions of the class for dependency resolution.
 */

#pragma once

#include "border_helper.h"
#include "../test/include/debug.hh"

namespace yakushima {

using key_slice_type = base_node::key_slice_type;
using key_length_type = base_node::key_length_type;
using value_align_type = base_node::value_align_type;
using value_length_type = base_node::value_length_type;

/**
 * start : forward declaration due to resolve dependency.
 */
template<class interior_node, class border_node>
static void border_split(border_node *border, std::string_view key_view, bool next_layer, void *value_ptr,
                         value_length_type value_length, value_align_type value_align,
                         std::vector<node_version64 *> &lock_list);

template<class interior_node, class border_node>
static void insert_lv(border_node *border, std::string_view key_view, bool next_layer, void *value_ptr,
                      value_length_type arg_value_length, value_align_type value_align,
                      std::vector<node_version64 *> &lock_list);
/**
 * end : forward declaration due to resolve dependency.
 */

/**
 * @details This may be called at split function.
 * It creates new interior node as parents of this interior_node and @a right.
 * @param[in] left
 * @param[in] right
 * @param[out] lock_list
 * @param[out] new_parent This function tells new parent to the caller via this argument.
 */
template<class interior_node, class border_node>
static void
create_interior_parent_of_interior(interior_node *left, interior_node *right, std::vector<node_version64 *> &lock_list,
                                   base_node **new_parent);

/**
 * @pre It already acquired lock of this node.
 * @details split interior node.
 * @param[in] interior
 * @param[in] child_node After split, it inserts this @a child_node.
 * @param[out] lock_list
 */
template<class interior_node, class border_node>
static void interior_split(interior_node *interior, base_node *child_node, std::vector<node_version64 *> &lock_list);

/**
 * @attention I have to traverse through the verification process. Because you might use an incorrect value.
 * @tparam interior_node
 * @tparam border_node
 * @param origin
 * @return
 */
template<class interior_node, class border_node>
std::tuple<key_slice_type, key_length_type>
find_lowest_key(base_node *origin);

template<class interior_node, class border_node>
static void
create_interior_parent_of_interior(interior_node *left, interior_node *right, std::vector<node_version64 *> &lock_list,
                                   base_node **new_parent) {
  left->set_version_root(false);
  right->set_version_root(false);
  interior_node *ni = new interior_node();
  ni->init_interior();
  ni->set_version_root(true);
  ni->set_version_inserting(true);
  ni->lock();
  lock_list.emplace_back(ni->get_version_ptr());
  /**
   * process base members
   */
  std::tuple<key_slice_type, key_length_type> pivot = find_lowest_key<interior_node, border_node>(right);
  ni->set_key(0, std::get<0>(pivot), std::get<1>(pivot));
  /**
   * process interior node members
   */
  ni->n_keys_increment();
  ni->set_child_at(0, left);
  ni->set_child_at(1, right);
  /**
   * release interior parent to global
   */
  left->set_parent(ni);
  right->set_parent(ni);
  *new_parent = ni;
  return;
}

template<class interior_node, class border_node>
static void interior_split(interior_node *interior, base_node *child_node, std::vector<node_version64 *> &lock_list) {
  interior_node *new_interior = new interior_node();
  new_interior->init_interior();
  /**
   * attention : After making changes to this node, it sets a splitting flag.
   * If a splitting flag is raised, the find_lowest_keys function may read the broken value.
   */
  interior->set_version_splitting(false);
  interior->set_version_root(false);
  /**
   * new interior is initially locked.
   */
  new_interior->set_version(interior->get_version());
  lock_list.emplace_back(new_interior->get_version_ptr());
  /**
   * split keys among n and n'
   */
  key_slice_type pivot_key = base_node::key_slice_length / 2;
  std::size_t split_children_points = pivot_key + 1;
  interior->move_key_to_base_range(new_interior, split_children_points);
  interior->set_n_keys(pivot_key);
  if (pivot_key % 2) {
    new_interior->set_n_keys(pivot_key);
  } else {
    new_interior->set_n_keys(pivot_key - 1);
  }
  interior->move_children_to_interior_range(new_interior, split_children_points);
  interior->set_key(pivot_key, 0, 0);

  interior->set_version_splitting(true);
  new_interior->set_version_splitting(true);
  /**
   * It inserts child_node.
   */
  std::tuple<key_slice_type, key_length_type> visitor = find_lowest_key<interior_node, border_node>(child_node);
  std::tuple<key_slice_type, key_length_type> pivot_view = find_lowest_key<interior_node, border_node>(new_interior);

  if (visitor < pivot_view) {
    interior->set_version_splitting(false);
    interior->template insert<border_node>(child_node);
    interior->set_version_splitting(true);
  } else {
    new_interior->set_version_splitting(false);
    new_interior->template insert<border_node>(child_node);
    new_interior->set_version_splitting(true);
  }


retry_lock_parent:
  base_node *p = interior->lock_parent();
  if (p == nullptr) {
    /**
     * The disappearance of the parent node may have made this node the root node in parallel.
     * It cares in below function.
     */
    create_interior_parent_of_interior<interior_node, border_node>(interior, new_interior, lock_list, &p);
    /**
     * p became new root.
     */
    base_node::set_root(p);
    return;
  }
  if (p != interior->get_parent()) {
    p->version_unlock();
    goto retry_lock_parent;
  }
  /**
   * p exists.
   */
  lock_list.emplace_back(p->get_version_ptr());
  if (p->get_version_border()) {
    border_node *pb = dynamic_cast<border_node *>(p);
    if (pb->get_permutation_cnk() == base_node::key_slice_length) {
      /**
       * parent border full case
       */
      create_interior_parent_of_interior<interior_node, border_node>(interior, new_interior, lock_list, &p);
      border_split<interior_node, border_node>(pb, std::string_view{reinterpret_cast<char *>(p->get_key_slice_at(0)),
                                                                    p->get_key_length_at(0)}, true, p, 0, 0,
                                               lock_list);
      return;
    }
    /**
     * parent border not-full case
     */
    insert_lv<interior_node, border_node>(pb,
                                          std::string_view{reinterpret_cast<char *>(new_interior->get_key_slice_at(0)),
                                                           new_interior->get_key_length_at(0)}, true, p, 0,
                                          0, lock_list);
    return;
  }
  interior_node *pi = dynamic_cast<interior_node *>(p);
  if (pi->get_n_keys() == base_node::key_slice_length) {
    /**
     * parent interior full case.
     */
    interior_split<interior_node, border_node>(pi, new_interior, lock_list);
    return;
  }
  /**
   * parent interior not-full case
   */
  pi->template insert<border_node>(new_interior);
  return;
}

template<class interior_node, class border_node>
std::tuple<key_slice_type, key_length_type>
find_lowest_key(base_node *origin) {
  for (;;) {
    base_node *bn = origin;
    for (;;) {
      node_version64_body v = bn->get_version();
      if ((v.get_locked() && ((v.get_inserting() && !v.get_splitting() && !v.get_deleting_node()))) ||
          // simple inserting
          (v.get_locked() && (!v.get_inserting() && !v.get_splitting() && !v.get_deleting_node()))) { // simple deleting
        _mm_pause();
        continue;
      }
      if (v.get_deleted()) {
        break;
      }
      if (bn->get_version_border()) {
        border_node *target = reinterpret_cast<border_node *>(bn);
        std::size_t low_pos = target->get_permutation_lowest_key_pos();
        if (bn->get_key_length_at(low_pos) <= sizeof(key_slice_type)) {
          key_slice_type kslice = bn->get_key_slice_at(low_pos);
          key_length_type klength = bn->get_key_length_at(low_pos);
          if (v == bn->get_version()) {
            return std::make_tuple(kslice, klength);
          }
        } else {
          base_node *ret = target->get_lv_at(low_pos)->get_next_layer();
          if (v == bn->get_version()) {
            bn = ret;
            continue;
          }
        }
        if (bn->get_version_deleted() ||
            (v.get_vsplit() != bn->get_version_vsplit())) {
          break;
        }
        continue;
      } else {
        base_node *ret = reinterpret_cast<interior_node *>(bn)->get_child_at(0);
        if (v == bn->get_version()) {
          bn = ret;
          continue;
        }
        if (bn->get_version_deleted() ||
            (v.get_vsplit() != bn->get_version_vsplit())) {
          break;
        }
        continue;
      }
    }
  }
}

} // namespace yakushima