/**
 * @file interior_helper.h
 * @details Declare functions that could not be member functions of the class for dependency resolution.
 */

#pragma once

#include "border_helper.h"

namespace yakushima {

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
create_interior_parent_of_interior(interior_node *left, interior_node *right,
                                   std::pair<base_node::key_slice_type, base_node::key_length_type> pivot_key,
                                   base_node **new_parent) {
  left->set_version_root(false);
  right->set_version_root(false);
  interior_node *ni = new interior_node(); // NOLINT
  ni->init_interior();
  ni->set_version_root(true);
  ni->set_version_inserting(true);
  ni->lock();
  /**
   * process base members
   */
  ni->set_key(0, pivot_key.first, pivot_key.second);
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
}

/**
 * @pre It already acquired lock of this node.
 * @details split interior node.
 * @param[in] interior
 * @param[in] child_node After split, it inserts this @a child_node.
 */
template<class interior_node, class border_node>
static void interior_split(interior_node *interior, base_node *child_node, std::pair<base_node::key_slice_type,
        base_node::key_length_type> pivot_key) {
  interior_node *new_interior = new interior_node(); // NOLINT
  new_interior->init_interior();
  /**
   * attention : After making changes to this node, it sets a splitting flag.
   * If a splitting flag is raised, the find_lowest_keys function may read the broken value.
   */
  interior->set_version_splitting(false);
  /**
   * new interior is initially locked.
   */
  new_interior->set_version(interior->get_version());
  /**
   * split keys among n and n'
   */
  key_slice_type pivot_key_pos = base_node::key_slice_length / 2;
  std::size_t split_children_points = pivot_key_pos + 1;
  interior->move_key_to_base_range(new_interior, split_children_points);
  interior->set_n_keys(pivot_key_pos);
  if (pivot_key_pos & 1) { // NOLINT
    new_interior->set_n_keys(pivot_key_pos);
  } else {
    new_interior->set_n_keys(pivot_key_pos - 1);
  }
  interior->move_children_to_interior_range(new_interior, split_children_points);
  std::pair<key_slice_type, key_length_type> pivot_view{interior->get_key_slice_at(pivot_key_pos),
                                                        interior->get_key_length_at(pivot_key_pos)};
  interior->set_key(pivot_key_pos, 0, 0);

  interior->set_version_splitting(true);
  new_interior->set_version_splitting(true);
  /**
   * It inserts child_node.
   */

  if (pivot_key < pivot_view) {
    interior->set_version_splitting(false);
    child_node->set_parent(interior);
    interior->template insert<border_node>(child_node, pivot_key);
    interior->set_version_splitting(true);
  } else {
    new_interior->set_version_splitting(false);
    child_node->set_parent(new_interior);
    new_interior->template insert<border_node>(child_node, pivot_key);
    new_interior->set_version_splitting(true);
  }

  base_node *p = interior->lock_parent();
  if (p == nullptr) {
#ifndef NDEBUG
    if (base_node::get_root() != interior) {
      std::cerr << __FILE__ << " : " << __LINE__ << " : " << std::endl;
      std::abort();
    }
#endif
    /**
     * The disappearance of the parent node may have made this node the root node in parallel.
     * It cares in below function.
     */
    create_interior_parent_of_interior<interior_node, border_node>(interior, new_interior, pivot_view, &p);
    interior->version_unlock();
    new_interior->version_unlock();
    /**
     * p became new root.
     */
    base_node::set_root(p);
    p->version_unlock();
    return;
  }
  /**
   * p exists.
   */
#ifndef NDEBUG
  if (p->get_version_deleted() ||
      p != interior->get_parent()) {
    std::cerr << __FILE__ << " : " << __LINE__ << " : " << std::endl;
    std::abort();
  }
#endif
  if (p->get_version_border()) {
    auto pb = dynamic_cast<border_node *>(p);
    pb->set_version_inserting(true);
    base_node *new_p{};
    create_interior_parent_of_interior<interior_node, border_node>(interior, new_interior, pivot_view, &new_p);
    interior->version_unlock();
    new_interior->version_unlock();
    link_or_value *lv = pb->get_lv(dynamic_cast<base_node *>(interior));
    p->version_atomic_inc_vdelete();
    lv->set_next_layer(new_p);
    new_p->set_parent(pb);
    new_p->version_unlock();
    p->version_unlock();
    return;
  }
  auto pi = dynamic_cast<interior_node *>(p);
  interior->version_unlock();
  new_interior->set_parent(pi);
  if (pi->get_n_keys() == base_node::key_slice_length) {
    /**
     * parent interior full case.
     */
    new_interior->version_unlock();
    interior_split<interior_node, border_node>(pi, new_interior, pivot_view);
    return;
  }
  /**
   * parent interior not-full case
   */
  new_interior->version_unlock();
  pi->template insert<border_node>(new_interior, pivot_view);
  pi->version_unlock();
}

} // namespace yakushima