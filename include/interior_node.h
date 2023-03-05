/**
 * @file interior_node.h
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>

#include "atomic_wrapper.h"
#include "base_node.h"
#include "destroy_manager.h"
#include "garbage_collection.h"
#include "interior_helper.h"
#include "log.h"
#include "thread_info.h"
#include "tree_instance.h"

#include "glog/logging.h"

namespace yakushima {

class alignas(CACHE_LINE_SIZE) interior_node final // NOLINT
    : public base_node {                           // NOLINT
public:
    /**
   * @details The structure is "ptr, key, ptr, key, ..., ptr".
   * So the child_length is key_slice_length plus 1.
   */
    static constexpr std::size_t child_length = key_slice_length + 1;
    using n_keys_body_type = std::uint8_t;
    using n_keys_type = std::atomic<n_keys_body_type>;

    ~interior_node() override{}; // NOLINT

    /**
   * @pre There is a child which is the same to @a child.
   * @post If the number of children is 1, It asks caller to make the child to root and
   * delete this node. Therefore, it place the-only-one child to position 0.
   * @details Delete operation on the element matching @a child.
   * @param[in] token
   * @param[in] child
   * @param[in] lock_list
   */
    template<class border_node>
    void delete_of(Token token, tree_instance* ti, base_node* const child) {
        set_version_inserting_deleting(true);
        std::size_t n_key = get_n_keys();
#ifndef NDEBUG
        if (n_key == 0) { LOG(ERROR) << log_location_prefix; }
#endif
        for (std::size_t i = 0; i <= n_key; ++i) {
            if (get_child_at(i) == child) {
                if (n_key == 1) {
                    set_version_deleted(true);
                    set_version_root(false);
                    n_keys_decrement();
                    base_node* pn = lock_parent();
                    if (pn == nullptr) {
                        get_child_at(!i)->atomic_set_version_root(true);
                        ti->store_root_ptr(get_child_at(!i)); // i == 0 or 1
                        get_child_at(!i)->set_parent(nullptr);
                    } else {
                        pn->set_version_inserting_deleting(true);
                        if (pn->get_version_border()) {
                            link_or_value* lv =
                                    dynamic_cast<border_node*>(pn)->get_lv(
                                            this);
                            base_node* sibling = get_child_at(!i);
                            lv->set_next_layer(sibling);
                            sibling->atomic_set_version_root(true);
                        } else {
                            dynamic_cast<interior_node*>(pn)->swap_child(
                                    this, get_child_at(!i));
                        }
                        get_child_at(!i)->set_parent(pn);
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

    /**
   * @brief release all heap objects and clean up.
   * @pre This function is called by single thread.
   */
    status destroy() override {
        std::vector<std::thread> th_vc;
        th_vc.reserve(n_keys_ + 1);
        for (auto i = 0; i < n_keys_ + 1; ++i) {
            auto process = [this, i] {
                get_child_at(i)->destroy();
                delete get_child_at(i); // NOLINT
            };
            if (destroy_manager::check_room()) {
                th_vc.emplace_back(process);
            } else {
                process();
            }
        }
        for (auto&& th : th_vc) {
            th.join();
            destroy_manager::return_room();
        }

        return status::OK_DESTROY_INTERIOR;
    }

    /**
   * @details display function for analysis and debug.
   */
    void display() override {
        display_base();

        std::cout << "interior_node::display" << std::endl;
        std::cout << "nkeys_ : " << std::to_string(get_n_keys()) << std::endl;
        for (std::size_t i = 0; i <= get_n_keys(); ++i) {
            std::cout << "child : " << i << " : " << get_child_at(i)
                      << std::endl;
        }
    }

    /**
     * @brief Collect the memory usage of this partial tree.
     * 
     * @param level the level of this node in the tree.
     * @param mem_stat the stack of memory usage for each level.
     */
    void mem_usage(std::size_t level,
                   memory_usage_stack& mem_stat) const override {
        if (mem_stat.size() <= level) { mem_stat.emplace_back(0, 0, 0); }
        auto& [node_num, used, reserved] = mem_stat.at(level);

        const auto n_keys = n_keys_ + 1UL;
        ++node_num;
        reserved += sizeof(interior_node);
        used += sizeof(interior_node) -
                ((child_length - n_keys) * sizeof(uintptr_t));

        const auto next_level = level + 1;
        for (std::size_t i = 0; i < n_keys; ++i) {
            get_child_at(i)->mem_usage(next_level, mem_stat);
        }
    }

    [[nodiscard]] n_keys_body_type get_n_keys() {
        return n_keys_.load(std::memory_order_acquire);
    }

    [[nodiscard]] base_node* get_child_at(std::size_t index) const {
        return loadAcquireN(children.at(index));
    }

    base_node* get_child_of(const key_slice_type key_slice,
                            const key_length_type key_length,
                            node_version64_body& v) {
        base_node* ret_child{};
        for (;;) {
            n_keys_body_type n_key = get_n_keys();
            ret_child = nullptr;
            for (auto i = 0; i < n_key; ++i) {
                std::size_t comp_length = key_length < get_key_length_at(i)
                                                  ? key_length
                                                  : get_key_length_at(i);
                int ret_memcmp = memcmp(&key_slice, &get_key_slice_ref().at(i),
                                        comp_length > sizeof(key_slice_type)
                                                ? sizeof(key_slice_type)
                                                : comp_length);
                if (ret_memcmp < 0 ||
                    (ret_memcmp == 0 && key_length < get_key_length_at(i))) {
                    /**
                     * The key_slice must be left direction of the index.
                     */
                    ret_child = children.at(i);
                    break;
                }
            }
            if (ret_child == nullptr) {
                /**
                 * The key_slice must be right direction of the index.
                 */
                ret_child = children.at(n_key);
                if (ret_child == nullptr) {
                    // SMOs have found, so retry from a root node
                    break;
                }
            }

            // get child's status before rechecking version
            node_version64_body child_v = ret_child->get_stable_version();
            node_version64_body check_v = get_stable_version();
            if (v == check_v && !child_v.get_deleted()) {
                v = child_v; // return child's version
                break;
            }
            if (v.get_vsplit() != check_v.get_vsplit() ||
                check_v.get_deleted()) {
                // SMOs have found, so retry from a root node
                ret_child = nullptr;
                break;
            }
            v = check_v;
        }
        return ret_child;
    }

    void init_interior() {
        init_base();
        set_version_border(false);
        children.fill(nullptr);
        set_n_keys(0);
    }

    /**
   * @pre It already acquired lock of this node.
   * @pre This interior node is not full.
   * @details insert @a child and fix @a children.
   * @param child new inserted child.
   */
    template<class border_node>
    void insert(base_node* const child,
                const std::pair<key_slice_type, key_length_type> pivot_key) {
        set_version_inserting_deleting(true);
        // std::tuple<key_slice_type, key_length_type> visitor =
        // std::make_tuple(pivot_key.first, pivot_key.second);
        key_slice_type key_slice{pivot_key.first};
        key_length_type key_length{pivot_key.second};
        n_keys_body_type n_key = get_n_keys();
        for (auto i = 0; i < n_key; ++i) {
            std::size_t comp_length{0};
            if (key_length > sizeof(key_slice_type) &&
                get_key_length_at(i) > sizeof(key_slice_type)) {
                comp_length = 8;
            } else {
                comp_length = key_length < get_key_length_at(i)
                                      ? key_length
                                      : get_key_length_at(i);
            }
            int ret_memcmp =
                    memcmp(&key_slice, &get_key_slice_ref().at(i), comp_length);
            if (ret_memcmp < 0 ||
                (ret_memcmp == 0 && key_length < get_key_length_at(i))) {
                if (i == 0) { // insert to child[0] or child[1].
                    shift_right_base_member(i, 1);
                    set_key(i, key_slice, key_length);
                    shift_right_children(i + 1);
                    set_child_at(i + 1, child);
                    n_keys_increment();
                    return;
                }
                // insert to middle points
                shift_right_base_member(i, 1);
                set_key(i, key_slice, key_length);
                shift_right_children(i + 1);
                set_child_at(i + 1, child);
                n_keys_increment();
                return;
            }
        }
        // insert to rightmost points
        set_key(n_key, key_slice, key_length);
        set_child_at(n_key + 1, child);
        n_keys_increment();
    }

    [[maybe_unused]] void
    move_children_to_interior_range(interior_node* const right_interior,
                                    const std::size_t start) {
        for (auto i = start; i < child_length; ++i) {
            right_interior->set_child_at(i - start, get_child_at(i));
            /**
       * right interiror is new parent of get_child_at(i). // NOLINT
       */
            get_child_at(i)->set_parent(right_interior);
            set_child_at(i, nullptr);
        }
    }

    void set_child_at(const std::size_t index, base_node* const new_child) {
        storeReleaseN(children.at(index), new_child);
    }

    void set_n_keys(const n_keys_body_type new_n_key) {
        n_keys_.store(new_n_key, std::memory_order_release);
    }

    /**
   * @pre It already acquired lock of this node.
   * @param start_pos
   * @param shift_size
   */
    void shift_left_children(const std::size_t start_pos,
                             const std::size_t shift_size) {
        for (std::size_t i = start_pos; i < child_length; ++i) {
            set_child_at(i - shift_size, get_child_at(i));
        }
    }

    /**
   * @pre It already acquired lock of this node.
   * It is not full-interior node.
   * @param start_pos
   * @param shift_size
   */
    void shift_right_children(const std::size_t start_pos) {
        std::size_t n_key = get_n_keys();
        for (std::size_t i = n_key + 1; i > start_pos; --i) {
            set_child_at(i, get_child_at(i - 1));
        }
    }

    void n_keys_decrement() { n_keys_.fetch_sub(1); }

    void n_keys_increment() { n_keys_.fetch_add(1); }

    void swap_child(base_node* const old_child, base_node* const new_child) {
        for (std::size_t i = 0; i < child_length; ++i) {
            if (get_child_at(i) == old_child) {
                set_child_at(i, new_child);
                return;
            }
        }
        /**
          * unreachable point.
          */
        LOG(ERROR) << log_location_prefix << "unreachable path";
    }

private:
    /**
     * first member of base_node is aligned along with cache line size.
     */

    /**
     * @attention This variable is read/written concurrently.
     */
    n_keys_type n_keys_{};
    /**
     * @attention This variable is read/written concurrently.
     */
    std::array<base_node*, child_length> children{};
};

} // namespace yakushima
