/**
 * @file border_helper.h
 * @details Declare functions that could not be member functions of the class for dependency resolution.
 */

#pragma once

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include "base_node.h"
#include "interior_helper.h"
#include "link_or_value.h"
#include "tree_instance.h"

namespace yakushima {

/**
 * forward declaration.
 */
template<class interior_node, class border_node>
static void
interior_split(tree_instance* ti, interior_node* interior, base_node* child_node, key_slice_type pivot_slice, // NOLINT
               key_length_type pivot_length);

/**
 * @pre It already locked this node.
 * @details border node split.
 * @param[in] border
 * @param[in] key_view
 * @param[in] value_ptr
 * @param[out] created_value_ptr The pointer to created value in yakushima.
 * @param[in] value_length
 * @param[in] value_align
 * @param[out] inserted_node_version_ptr
 * @param[in] rank
 */
template<class interior_node, class border_node>
static void
border_split(tree_instance* ti, border_node* border, std::string_view key_view, void* value_ptr, void** created_value_ptr, // NOLINT
             value_length_type value_length, value_align_type value_align, node_version64** inserted_node_version_ptr, std::size_t rank);

/**
 * Start impl.
 */

/**
 * @details This may be called at split function.
 * It creates new interior node as parents of this border_node and @a higher_border node.
 * After that, it inserts based on @a key_view, @a value_ptr, ... (args).
 * @param[in] left
 * @param[in] right This is a higher border_node as result of split for this node.
 * @param[out] lock_list This is unused because the border nodes is not-full as result of split.
 * @param[out] new_parent This is a new parents.
 * The insert_lv function needs lock_list as an argument, so it is passed in spite of not using.
 */

template<class interior_node, class border_node>
static void
create_interior_parent_of_border(border_node* const left, border_node* const right, interior_node** const new_parent) {
    left->set_version_root(false);
    right->set_version_root(false);
    /**
     * create a new interior node p with children n, n'
     */
    auto ni = new interior_node(); // NOLINT
    ni->init_interior();
    ni->set_version_root(true);
    ni->set_version_inserting_deleting(true);
    ni->lock();
    /**
     * process base node members
     */
    ni->set_key(0, right->get_key_slice_at(0), right->get_key_length_at(0));
    /**
     * process interior node members
     */
    ni->set_child_at(0, left);
    ni->set_child_at(1, right);
    ni->n_keys_increment();
    /**
     * release interior parent to global.
     */
    left->set_parent(ni);
    right->set_parent(ni);
    *new_parent = ni;
}

/**
 * @pre It already locked @a border.
 * @details This function is also called when creating a new layer when 8 bytes-key collides at a border node.
 * At that time, the original value is moved to the new layer.
 * This function does not use a template declaration because its pointer is retrieved with void *.
 * @param[in] border
 * @param[in] key_view
 * @param[in] value_ptr
 * @param[out] created_value_ptr
 * @param[in] arg_value_length
 * @param[in] value_align
 * @param[out] inserted_node_version_ptr
 * @param[in] rank
 */

template<class interior_node, class border_node>
static void
insert_lv(tree_instance* ti, border_node* const border, std::string_view key_view, void* const value_ptr, void** const created_value_ptr,
          const value_length_type arg_value_length, const value_align_type value_align,
          node_version64** inserted_node_version_ptr, std::size_t rank) {
    border->set_version_inserting_deleting(true);
    std::size_t cnk = border->get_permutation_cnk();
    if (cnk == base_node::key_slice_length) {
        /**
         * It needs splitting
         */
        border_split<interior_node, border_node>(ti, border, key_view, value_ptr, created_value_ptr, arg_value_length,
                                                 value_align, inserted_node_version_ptr, rank);
    } else {
        /**
         * Insert into this nodes.
         */
        if (inserted_node_version_ptr != nullptr) {
            *inserted_node_version_ptr = border->get_version_ptr();
        }
        border->insert_lv_at(cnk, key_view, value_ptr, created_value_ptr, arg_value_length, value_align, rank);
        border->version_unlock();
    }
}

template<class interior_node, class border_node>
static void
border_split(tree_instance* ti, border_node* const border, std::string_view key_view, void* const value_ptr,
             void** const created_value_ptr, const value_length_type value_length, const value_align_type value_align,
             node_version64** inserted_node_version_ptr, std::size_t rank) {
    border->set_version_splitting(true);
    border_node* new_border = new border_node(); // NOLINT
    new_border->init_border();
    new_border->set_next(border->get_next());
    new_border->set_prev(border);

    /**
     * new border is initially locked
     */
    new_border->set_version(border->get_version());
    border->set_next(new_border);
    if (new_border->get_next() != nullptr) {
        /**
         * The prev of border next can be updated if it posesses the border lock.
         */
        new_border->get_next()->set_prev(new_border);
    }
    /**
     * split keys among n and n'
     */
    constexpr std::size_t key_tuple_index = 0;
    constexpr std::size_t key_pos = 1;
    std::vector<std::tuple<base_node::key_tuple, std::uint8_t>> vec;
    std::uint8_t cnk = border->get_permutation_cnk();
    vec.reserve(cnk);
    for (std::uint8_t i = 0; i < cnk; ++i) {
        vec.emplace_back(base_node::key_tuple(border->get_key_slice_at(i), border->get_key_length_at(i)), i); // NOLINT
    }
    std::sort(vec.begin(), vec.end());
    /**
     * split
     * If the fan-out is odd, keep more than half to improve the performance.
     */
    std::size_t remaining_size = base_node::key_slice_length / 2 + 1;

    std::size_t index_ctr(0);
    std::vector<std::size_t> shift_pos;
    for (auto itr = vec.begin() + remaining_size; itr != vec.end(); ++itr) {
        /**
         * move base_node members to new nodes
         */
        new_border->set_key_slice_at(index_ctr, std::get<key_tuple_index>(*itr).get_key_slice());
        new_border->set_key_length_at(index_ctr, std::get<key_tuple_index>(*itr).get_key_length());
        new_border->set_lv(index_ctr, border->get_lv_at(std::get<key_pos>(*itr)));
        base_node* nl = border->get_lv_at(std::get<key_pos>(*itr))->get_next_layer();
        if (nl != nullptr) {
            nl->set_parent(new_border);
        }
        shift_pos.emplace_back(std::get<key_pos>(*itr));
        ++index_ctr;
    }
    /**
     * fix member positions of old border_node.
     */
    std::sort(shift_pos.begin(), shift_pos.end());
    std::size_t shifted_ctr(0);
    for (std::size_t& shift_po : shift_pos) {
        border->shift_left_base_member(shift_po + 1 - shifted_ctr, 1);
        border->shift_left_border_member(shift_po + 1 - shifted_ctr, 1);
        ++shifted_ctr;
    }
    /**
     * maintenance about empty parts due to new border.
     */
    border->init_base_member_range(remaining_size);
    border->init_border_member_range(remaining_size);
    /**
     * fix permutations
     */
    border->set_permutation_cnk(remaining_size);
    border->permutation_rearrange();
    new_border->set_permutation_cnk(base_node::key_slice_length - remaining_size);
    new_border->permutation_rearrange();

    /**
     * The insert process we wanted to do before we split.
     * key_slice must be initialized to 0.
     */
    key_slice_type key_slice{0};
    key_length_type key_length; // NOLINT
    if (key_view.size() > sizeof(key_slice_type)) {
        memcpy(&key_slice, key_view.data(), sizeof(key_slice_type));
        key_length = sizeof(key_slice_type) + 1;
    } else {
        if (!key_view.empty()) {
            memcpy(&key_slice, key_view.data(), key_view.size());
        }
        key_length = static_cast<key_length_type>(key_view.size());
    }
    int ret_memcmp{memcmp(&key_slice, &new_border->get_key_slice_ref().at(0),
                          (key_length > sizeof(key_slice_type) &&
                           new_border->get_key_length_at(0) > sizeof(key_slice_type))
                                  ? sizeof(key_slice_type)
                          : key_length < new_border->get_key_length_at(0) ? key_length
                                                                          : new_border->get_key_length_at(0))};
    if (key_length == 0 || ret_memcmp < 0 || (ret_memcmp == 0 && key_length < new_border->get_key_length_at(0))) {
        /**
         * insert to lower border node.
         */
        if (inserted_node_version_ptr != nullptr) {
            *inserted_node_version_ptr = border->get_version_ptr();
        }
        border->insert_lv_at(remaining_size, key_view, value_ptr, created_value_ptr, value_length, value_align, rank);
    } else {
        /**
         * insert to higher border node.
         */
        if (inserted_node_version_ptr != nullptr) {
            *inserted_node_version_ptr = new_border->get_version_ptr();
        }
        new_border->insert_lv_at(base_node::key_slice_length - remaining_size, key_view, value_ptr, created_value_ptr,
                                 value_length, value_align, rank - remaining_size);
    }

    base_node* p = border->lock_parent();
    if (p == nullptr) {
#ifndef NDEBUG
        if (ti->load_root_ptr() != border) {
            std::cerr << __FILE__ << " : " << __LINE__ << " : " << std::endl;
            std::abort();
        }
#endif
        /**
         * create interior as parents and insert k.
         * The disappearance of the parent node may have made this node the root node in parallel.
         * It cares in below function.
         */
        create_interior_parent_of_border<interior_node, border_node>(border, new_border,
                                                                     reinterpret_cast<interior_node**>(&p)); // NOLINT
        border->version_unlock();
        new_border->version_unlock();
        ti->store_root_ptr(p);
        p->version_unlock();
        return;
    }

#ifndef NDEBUG
    if (p != border->get_parent()) {
        std::cerr << __FILE__ << " : " << __LINE__ << " : " << std::endl;
        std::abort();
    }
#endif

    if (p->get_version_border()) {
        /**
         * parent is border node.
         * The old border node which is before this split was root of the some layer.
         * So it creates new interior nodes in the layer and insert its interior pointer
         * to the (parent) border node.
         * attention : The parent border node had this border node as one of the next_layer before the split.
         * The pointer is exchanged for a new parent interior node.
         */
        auto pb = dynamic_cast<border_node*>(p);
        pb->set_version_inserting_deleting(true);
        interior_node* pi{};
        create_interior_parent_of_border<interior_node, border_node>(border, new_border, &pi);
        border->version_unlock();
        new_border->version_unlock();
        pi->set_parent(p);
        pi->version_unlock();
        link_or_value* lv = pb->get_lv(border);
        lv->set_next_layer(pi);
        p->version_unlock();
        return;
    }
    /**
     * parent is interior node.
     */
#ifndef NDEBUG
    if (p->get_version_deleted()) {
        std::cerr << __FILE__ << " : " << __LINE__ << " : " << std::endl;
        std::abort();
    }
#endif
    auto pi = dynamic_cast<interior_node*>(p);
    border->set_version_root(false);
    new_border->set_version_root(false);
    border->version_unlock();
    new_border->version_unlock();
    if (pi->get_n_keys() == base_node::key_slice_length) {
        /**
         * interior full case, it splits and inserts.
         */
        interior_split<interior_node, border_node>(ti, pi, reinterpret_cast<base_node*>(new_border), // NOLINT
                                                   std::make_pair(new_border->get_key_slice_at(0),
                                                                  new_border->get_key_length_at(0)));
        return;
    }
    /**
     * interior not-full case, it inserts.
     */
    new_border->set_parent(pi);
    pi->template insert<border_node>(new_border,
                                     std::make_pair(new_border->get_key_slice_at(0), new_border->get_key_length_at(0)));
    pi->version_unlock();
}

} // namespace yakushima
