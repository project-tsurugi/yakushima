/**
 * @file interior_helper.h
 * @details Declare functions that could not be member functions of the class for
 * dependency resolution.
 */

#pragma once

#include "border_helper.h"
#include "interior_node.h"
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
 * @pre It already acquired lock of @a interior, and released lock of @a child_node.
 * @details split interior node.
 * @param[in] interior
 * @param[in] child_node After split, it inserts this @a child_node.
 */
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
    new_interior->set_n_keys(key_slice_length - pivot_key_pos - 1);
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
        child_node->set_parent(interior); // guard by parent lock
        interior->insert(child_node, inserting_key);
    } else {
        child_node->set_parent(new_interior); // guard by parent lock
        new_interior->insert(child_node, inserting_key);
    }

    base_node* p = interior->lock_parent(ti);
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
        create_interior_parent_of_interior(
                interior, new_interior, std::make_pair(pivot_key, pivot_length),
                &p);
        interior->version_unlock();
        new_interior->version_unlock();
        /**
         * p became new root.
         */
        ti->store_root_ptr(p);
        p->version_unlock();
        ti->root_unlock();
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
        auto* pb = dynamic_cast<border_node*>(p);
        base_node* new_p{};
        create_interior_parent_of_interior(
                interior, new_interior, std::make_pair(pivot_key, pivot_length),
                &new_p);
        interior->version_unlock();
        new_interior->version_unlock();
        link_or_value* lv = pb->get_lv(interior);
        lv->set_next_layer(new_p);
        new_p->set_parent(pb); // guard by parent lock
        new_p->version_unlock();
        p->version_unlock();
        return;
    }
    auto* pi = dynamic_cast<interior_node*>(p);
    interior->version_unlock();
    new_interior->set_parent(pi); // guard by parent lock
    new_interior->version_unlock();
    if (pi->get_n_keys() == key_slice_length) {
        /**
         * parent interior full case.
         */
        interior_split(
                ti, pi, new_interior, std::make_pair(pivot_key, pivot_length));
        return;
    }
    /**
     * parent interior not-full case
     */
    pi->insert(new_interior, std::make_pair(pivot_key, pivot_length));
    pi->version_unlock();
}

inline
    void interior_node::delete_of(Token token, tree_instance* ti, base_node* const child) {
        set_version_inserting_deleting(true);
        std::size_t n_key = get_n_keys();
#ifndef NDEBUG
        if (n_key == 0) { LOG(ERROR) << log_location_prefix; }
#endif
        for (std::size_t i = 0; i <= n_key; ++i) {
            if (get_child_at(i) == child) {
                if (n_key == 1) {
                    // remove this node and promote its last child node one level
                    set_version_deleted(true);
                    n_keys_decrement();
                    base_node* sibling = get_child_at(1 - i); // i == 0 or 1
                    base_node* pn = lock_parent(ti);
                    if (pn == nullptr) { // if this node is masstree root
                        set_version_root(false); // guard by root lock
                        sibling->atomic_set_version_root(true); // guard by root lock
                        ti->store_root_ptr(sibling); // guard by root lock
                        sibling->set_parent(nullptr); // guard by root lock
                        ti->root_unlock();
                    } else {
                        set_version_root(false); // guard by parent lock
                        //pn->set_version_inserting_deleting(true);
                        if (pn->get_version_border()) { // if this node is layer 1+ root
                            link_or_value* lv =
                                    dynamic_cast<border_node*>(pn)->get_lv(
                                            this);
                            lv->set_next_layer(sibling);
                            sibling->atomic_set_version_root(true); // guard by parent lock
                        } else {
                            dynamic_cast<interior_node*>(pn)->swap_child(this, sibling);
                        }
                        sibling->set_parent(pn); // guard by parent lock
                        pn->version_unlock();
                    }
                    version_unlock();
                    auto* tinfo =
                            reinterpret_cast<thread_info*>(token); // NOLINT
                    tinfo->get_gc_info().push_node_container(
                            std::tuple{tinfo->get_begin_epoch(), this});
                } else {          // n_key > 1
                    if (i == 0) { // leftmost points
                        shift_left_base_member(1, 1);
                        shift_left_children(1, 1);
                        set_child_at(n_key, nullptr);
                    } else if (i == n_key) { // rightmost points
                        // no unique process
                        set_child_at(i, nullptr);
                    } else { // middle points
                        shift_left_base_member(i, 1);
                        shift_left_children(i + 1, 1);
                        set_child_at(n_key, nullptr);
                    }
                    set_key(n_key - 1, 0, 0);
                    n_keys_decrement();
                    version_unlock();
                }
                return;
            }
        }

#ifndef NDEBUG
        LOG(ERROR) << log_location_prefix << "precondition error";
#endif
    }

} // namespace yakushima
