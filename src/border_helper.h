/**
 * @file border_helper.h
 * @details Declare functions that could not be member functions of the class for dependency resolution.
 */

#pragma once

#include "interior_helper.h"
#include "border_node.h"

#include "../test/include/debug.hh"

namespace yakushima {

using key_slice_type = base_node::key_slice_type;
using key_length_type = base_node::key_length_type;
using value_align_type = base_node::value_align_type;
using value_length_type = base_node::value_length_type;

/**
 * @details This may be called at split function.
 * It creates new interior node as parents of this border_node and @a higher_border node.
 * After that, it inserts based on @a key_view, @a value_ptr, ... (args).
 * @param[in] left
 * @param[in] right This is a higher border_node as result of split for this node.
 * @param[in] key_view
 * @param[in] value_ptr
 * @param[in] value_length
 * @param[in] value_align
 * @param[out] lock_list This is unused because the border nodes is not-full as result of split.
 * @param[out] new_parent This is a new parents.
 * The insert_lv function needs lock_list as an argument, so it is passed in spite of not using.
 */
static void create_interior_parents_and_insert(border_node *left,
                                               border_node *right,
                                               std::string_view key_view, bool next_layer, void *value_ptr,
                                               value_length_type value_length,
                                               value_align_type value_align, std::vector<node_version64 *> &lock_list,
                                               interior_node **new_parent);

/**
 * @pre It already locked @a border.
 * @details This function is also called when creating a new layer when 8 bytes-key collides at a border node.
 * At that time, the original value is moved to the new layer.
 * This function does not use a template declaration because its pointer is retrieved with void *.
 * @param[out] lock_list Hold the lock so that the caller can release the lock from below.
 */
static void insert_lv(border_node *border, std::string_view key_view, bool next_layer, void *value_ptr,
                      value_length_type arg_value_length, value_align_type value_align,
                      std::vector<node_version64 *> &lock_list);

/**
 * @pre It already locked this node.
 * @details border node split.
 * @param[in] key_view
 * @param[in] value_ptr
 * @param[in] value_length
 * @param[in] value_align
 * @param[out] lock_list Hold the lock so that the caller can release the lock from below.
 */
static void border_split(border_node *border,
                         std::string_view key_view,
                         bool next_layer,
                         void *value_ptr,
                         value_length_type value_length,
                         value_align_type value_align,
                         std::vector<node_version64 *> &lock_list);

/**
 * Start impl.
 */

static void create_interior_parents_and_insert(border_node *left,
                                               border_node *right,
                                               std::string_view key_view, bool next_layer, void *value_ptr,
                                               value_length_type value_length,
                                               value_align_type value_align, std::vector<node_version64 *> &lock_list,
                                               interior_node **new_parent) {
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
  ni->set_key(0, right->get_key_slice_at(0), right->get_key_length_at(0));
  /**
   * process interior node members
   */
  ni->n_keys_increment();
  ni->set_child_at(0, dynamic_cast<base_node *>(left));
  ni->set_child_at(1, dynamic_cast<base_node *>(right));
  /**
   * The insert process we wanted to do before we split.
   * key_slice must be initialized to 0.
   */
  key_slice_type key_slice(0);
  key_length_type key_length(0);
  if (key_view.size() > sizeof(key_slice_type)) {
    memcpy(&key_slice, key_view.data(), sizeof(key_slice_type));
    key_length = sizeof(key_slice_type);
  } else {
    if (key_view.size() > 0) {
      memcpy(&key_slice, key_view.data(), key_view.size());
    }
    key_length = key_view.size();
  }
  std::tuple<key_slice_type, key_length_type> visitor{key_slice, key_length};
  std::tuple<key_slice_type, key_length_type> r_low{right->get_key_slice_at(0), right->get_key_length_at(0)};
  if (visitor < r_low) {
    /**
     * insert to lower border node.
     * @attention lock_list will not be added new lock.
     */
    insert_lv(left, key_view, next_layer, value_ptr, value_length, value_align, lock_list);
  } else if (visitor > r_low) {
    /**
     * insert to higher border node.
     * @attention lock_list will not be added new lock.
     */
    insert_lv(right, key_view, next_layer, value_ptr, value_length, value_align, lock_list);
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
  left->set_parent(dynamic_cast<base_node *>(ni));
  right->set_parent(dynamic_cast<base_node *>(ni));
  *new_parent = ni;
  base_node::set_root(dynamic_cast<base_node *>(ni));
}

static void insert_lv(border_node *border,
                      std::string_view key_view,
                      bool next_layer,
                      void *value_ptr,
                      value_length_type arg_value_length,
                      value_align_type value_align,
                      std::vector<node_version64 *> &lock_list) {
  border->set_version_inserting(true);
  std::size_t cnk = border->get_permutation_cnk();
  if (cnk == base_node::key_slice_length) {
    /**
     * It needs splitting
     */
    border_split(border, key_view, next_layer, value_ptr, arg_value_length, value_align, lock_list);
  } else {
    /**
     * Insert into this nodes.
     */
    border->insert_lv_at(cnk, key_view, next_layer, value_ptr, arg_value_length, value_align);
  }
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
static void border_split(border_node *border,
                         std::string_view key_view,
                         bool next_layer,
                         void *value_ptr,
                         value_length_type value_length,
                         value_align_type value_align,
                         std::vector<node_version64 *> &lock_list) {
  border_node *new_border = new border_node();
  new_border->init_border();
  border->set_next(new_border);
  new_border->set_prev(border);
  border->set_version_root(false);
  border->set_version_splitting(true);
  /**
   * new border is initially locked
   */
  new_border->set_version(border->get_version());
  lock_list.emplace_back(new_border->get_version_ptr());
  /**
   * split keys among n and n'
   */
  constexpr std::size_t key_slice_index = 0;
  constexpr std::size_t key_length_index = 1;
  constexpr std::size_t key_pos = 2;
  std::vector<std::tuple<key_slice_type, key_length_type, std::uint8_t>> vec;
  std::uint8_t cnk = border->get_permutation_cnk();
  vec.reserve(cnk);
  key_slice_type *key_slice_array = border->get_key_slice();
  key_length_type *key_length_array = border->get_key_length();
  for (std::uint8_t i = 0; i < cnk; ++i) {
    vec.emplace_back(key_slice_array[i], key_length_array[i], i);
  }
  std::sort(vec.begin(), vec.end());
  /**
   * split
   */
  std::size_t index_ctr(0);
  std::vector<std::size_t> shift_pos;
  for (auto itr = vec.begin() + (base_node::key_slice_length / 2); itr != vec.end(); ++itr) {
    /**
     * move base_node members to new nodes
     */
    new_border->set_key_slice_at(index_ctr, std::get<key_slice_index>(*itr));
    new_border->set_key_length_at(index_ctr, std::get<key_length_index>(*itr));
    new_border->set_lv(index_ctr, border->get_lv_at(std::get<key_pos>(*itr)));
    if (std::get<key_pos>(*itr) < (base_node::key_slice_length / 2)) {
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
    border->shift_left_base_member(*itr + 1 - shifted_ctr, 1);
    border->shift_left_border_member(*itr + 1 - shifted_ctr, 1);
    ++shifted_ctr;
  }
  /**
   * maintenance about empty parts due to new border.
   */
  border->init_base_member_range(base_node::key_slice_length / 2, base_node::key_slice_length - 1);
  border->init_border_member_range(base_node::key_slice_length / 2, base_node::key_slice_length - 1);
  /**
   * fix permutations
   */
  border->set_permutation_cnk(base_node::key_slice_length / 2);
  border->set_permutation_rearrange();
  new_border->set_permutation_cnk(base_node::key_slice_length - base_node::key_slice_length / 2);
  new_border->set_permutation_rearrange();

  base_node *p = border->lock_parent();
  if (p == nullptr) {
    /**
     * create interior as parents and insert k.
     */
    create_interior_parents_and_insert(border, new_border, key_view, next_layer, value_ptr, value_length, value_align,
                                       lock_list, reinterpret_cast<interior_node**>(&p));
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
    interior_node *pi;
    create_interior_parents_and_insert(border, new_border, key_view, next_layer, value_ptr, value_length,
                                       value_align, lock_list, &pi);
    if (pb->get_permutation_cnk() == base_node::key_slice_length) {
      /**
       * border full case, it splits and inserts.
       */
      border_split(pb, key_view, true, pi, value_length, value_align, lock_list);
      return;
    }
    /**
     * border not-full case, it inserts.
     */
    insert_lv(pb, std::string_view
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
    interior_split(pi, new_border, lock_list);
    return;
  }
  /**
   * interior not-full case, it inserts.
   */
  pi->set_version_inserting(true);
  pi->insert(new_border);
  return;
}

} // namespace yakushima