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
#include "link_or_value.h"
#include "log.h"
#include "thread_info.h"
#include "tree_instance.h"

#include "glog/logging.h"

namespace yakushima {

class locked_border_node;
class locked_interior_node;

class alignas(CACHE_LINE_SIZE) interior_node // NOLINT
    : public base_node {                     // NOLINT
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

        std::cout << "interior_node::display\n";
        std::cout << "nkeys_ : " << std::to_string(get_n_keys()) << "\n";
        for (std::size_t i = 0; i <= get_n_keys(); ++i) {
            std::cout << "child : " << i << " : " << get_child_at(i) << "\n";
        }
        std::cout << std::flush;
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

    // base_node methods
    [[nodiscard]] locked_interior_node* lock() {
        base_node::lock();
        return reinterpret_cast<locked_interior_node*>(this); // NOLINT
    }
    void set_parent(std::nullptr_t){base_node::set_parent(nullptr);}
    void set_parent(locked_interior_node* p){base_node::set_parent(reinterpret_cast<interior_node*>(p));} // NOLINT
    //void set_parent(locked_border_node* p){base_node::set_parent(reinterpret_cast<border_node*>(p));} // NOLINT
    void set_parent(locked_border_node* p){base_node::set_parent(reinterpret_cast<base_node*>(p));} // NOLINT
    void version_unlock() = delete;
    void shift_left_base_member(std::size_t, std::size_t) = delete;
    void shift_right_base_member(std::size_t, std::size_t) = delete;

// NOLINTBEGIN(*-non-private-*)
protected:
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
// NOLINTEND(*-non-private-*)
};

class alignas(CACHE_LINE_SIZE) locked_interior_node : public interior_node {
public:
    void lock() = delete;
    interior_node* version_unlock() {
        base_node::version_unlock();
        return static_cast<interior_node*>(this);
    }
    void shift_left_base_member(std::size_t start, std::size_t sz) { base_node::shift_left_base_member(start, sz); };
    void shift_right_base_member(std::size_t start, std::size_t sz) { base_node::shift_right_base_member(start, sz); };

    void init_interior() {
        init_base();
        set_version_border(false);
        children.fill(nullptr);
        set_n_keys(0);
    }

    /**
     * @pre There is a child which is the same to @a child.
     * @a this interior node is locked by caller.
     * @details Delete operation on the element matching @a child.
     * If after deletion there is only one child left,
     * promote the child to the position of @a this node.
     * @param[in] token
     * @param[in] ti pointer to the masstree instance
     * @param[in] child removed child node (not locked by caller)
     */
    void delete_of(Token token, tree_instance* ti, base_node* child);

    /**
     * @pre It already acquired lock of this node.
     * @pre This interior node is not full.
     * @details insert @a child and fix @a children.
     * @param child new inserted child.
     */
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
    move_children_to_interior_range(locked_interior_node* const right_interior,
                                    const std::size_t start) {
        for (auto i = start; i < child_length; ++i) {
            right_interior->set_child_at(i - start, get_child_at(i));
            /**
             * right interiror is new parent of get_child_at(i). // NOLINT
             */
            get_child_at(i)->set_parent(right_interior); // guard by parent lock
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

};

// localy created, not exposed object : can call set_* methods without lock
class alignas(CACHE_LINE_SIZE) new_interior_node : public locked_interior_node {
    // yakushima uses typeof() compare, so create as interior_node class
    new_interior_node() = delete;
public:
    static new_interior_node* create() { return of(new interior_node()); }
    static new_interior_node* of(interior_node* p) { return reinterpret_cast<new_interior_node*>(p); } // NOLINT


    [[nodiscard]] locked_interior_node* lock() { return interior_node::lock(); }
    void version_unlock() = delete;
};

static_assert(sizeof(new_interior_node) == sizeof(interior_node));

} // namespace yakushima
