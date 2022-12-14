/**
 * @file interior_helper.h
 * @details Declare functions that could not be member functions of the class for
 * dependency resolution.
 */

#pragma once

#include "border_helper.h"
#include "log.h"
#include "tree_instance.h"

#include "glog/logging.h"

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
static void create_interior_parent_of_interior(
        interior_node* const left, interior_node* const right,
        const std::pair<key_slice_type, key_length_type> pivot_key,
        base_node** const new_parent) {
    left->set_version_root(false);
    right->set_version_root(false);
    interior_node* ni = new interior_node(); // NOLINT
    ni->init_interior();
    ni->set_version_root(true);
    ni->set_version_inserting_deleting(true);
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
static void
interior_split(tree_instance* ti, interior_node* const interior,
               base_node* const child_node,
               const std::pair<key_slice_type, key_length_type> inserting_key) {
    interior->set_version_splitting(true);
    interior_node* new_interior = new interior_node(); // NOLINT
    new_interior->init_interior();

    /**
   * new interior is initially locked.
   */
    new_interior->set_version(interior->get_version());
    /**
   * split keys among n and n'
   */
    key_slice_type pivot_key_pos = key_slice_length / 2;
    std::size_t split_children_points = pivot_key_pos + 1;
    interior->move_key_to_base_range(new_interior, split_children_points);
    interior->set_n_keys(pivot_key_pos);
    if (pivot_key_pos & 1) { // NOLINT
        new_interior->set_n_keys(pivot_key_pos);
    } else {
        new_interior->set_n_keys(pivot_key_pos - 1);
    }
    interior->move_children_to_interior_range(new_interior,
                                              split_children_points);
    key_slice_type pivot_key = interior->get_key_slice_at(pivot_key_pos);
    key_length_type pivot_length = interior->get_key_length_at(pivot_key_pos);
    interior->set_key(pivot_key_pos, 0, 0);

    /**
   * It inserts child_node.
   */

    key_slice_type key_slice{inserting_key.first};
    key_length_type key_length{inserting_key.second};
#ifndef NDEBUG
    if (key_length == 0 || pivot_length == 0) {
        LOG(ERROR) << log_location_prefix;
    }
#endif
    std::size_t comp_length{0};
    if (key_length > sizeof(key_slice_type) &&
        pivot_length > sizeof(key_slice_type)) {
        comp_length = 8;
    } else {
        comp_length = key_length < pivot_length ? key_length : pivot_length;
    }
    int ret_memcmp = memcmp(&key_slice, &pivot_key, comp_length);
    if (ret_memcmp < 0 || (ret_memcmp == 0 && key_length < pivot_length)) {
        child_node->set_parent(interior);
        interior->template insert<border_node>(child_node, inserting_key);
    } else {
        child_node->set_parent(new_interior);
        new_interior->template insert<border_node>(child_node, inserting_key);
    }

    base_node* p = interior->lock_parent();
    if (p == nullptr) {
#ifndef NDEBUG
        if (ti->load_root_ptr() != interior) {
            LOG(ERROR) << log_location_prefix;
        }
#endif
        /**
     * The disappearance of the parent node may have made this node the root node in
     * parallel. It cares in below function.
     */
        create_interior_parent_of_interior<interior_node, border_node>(
                interior, new_interior, std::make_pair(pivot_key, pivot_length),
                &p);
        interior->version_unlock();
        new_interior->version_unlock();
        /**
     * p became new root.
     */
        ti->store_root_ptr(p);
        p->version_unlock();
        return;
    }
    /**
   * p exists.
   */
#ifndef NDEBUG
    if (p->get_version_deleted() || p != interior->get_parent()) {
        LOG(ERROR) << log_location_prefix;
    }
#endif
    if (p->get_version_border()) {
        p->set_version_inserting_deleting(true);
        auto* pb = dynamic_cast<border_node*>(p);
        base_node* new_p{};
        create_interior_parent_of_interior<interior_node, border_node>(
                interior, new_interior, std::make_pair(pivot_key, pivot_length),
                &new_p);
        interior->version_unlock();
        new_interior->version_unlock();
        link_or_value* lv = pb->get_lv(interior);
        lv->set_next_layer(new_p);
        new_p->set_parent(pb);
        new_p->version_unlock();
        p->version_unlock();
        return;
    }
    auto* pi = dynamic_cast<interior_node*>(p);
    interior->version_unlock();
    new_interior->set_parent(pi);
    new_interior->version_unlock();
    if (pi->get_n_keys() == key_slice_length) {
        /**
     * parent interior full case.
     */
        interior_split<interior_node, border_node>(
                ti, pi, new_interior, std::make_pair(pivot_key, pivot_length));
        return;
    }
    /**
   * parent interior not-full case
   */
    pi->template insert<border_node>(new_interior,
                                     std::make_pair(pivot_key, pivot_length));
    pi->version_unlock();
}

} // namespace yakushima
