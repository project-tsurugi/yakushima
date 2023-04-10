/**
 * @file border_node.h
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>

#include "atomic_wrapper.h"
#include "garbage_collection.h"
#include "interior_node.h"
#include "link_or_value.h"
#include "permutation.h"
#include "thread_info.h"

#include "glog/logging.h"

namespace yakushima {

using std::cout;
using std::endl;

class alignas(CACHE_LINE_SIZE) border_node final : public base_node { // NOLINT
public:
    ~border_node() override {} // NOLINT

    /**
      * @pre This function is called by delete_of function.
      * @details delete the key-value corresponding to @a pos as position.
      * @param[in] rank
      * @param[in] pos The position of being deleted.
      * @param[in] target_is_value
      */
    void delete_at(Token token, const std::size_t rank, const std::size_t pos,
                   const bool target_is_value) {
        auto* ti = reinterpret_cast<thread_info*>(token); // NOLINT
        if (target_is_value) {
            value* vp = lv_.at(pos).get_value();
            if (value::is_value_ptr(vp)) {
                value::remove_delete_flag(vp);
                auto [v_ptr, v_len, v_align] = value::get_gc_info(vp);
                ti->get_gc_info().push_value_container(
                        {ti->get_begin_epoch(), v_ptr, v_len, v_align});
            }
        }

        // rearrange permutation
        permutation_.delete_rank(rank);
        permutation_.dec_key_num();
    }

    /**
      * @pre There is a lv which points to @a child.
      * @details Delete operation on the element matching @a child.
      * @param[in] token
      * @param[in] child
      */
    void delete_of(Token token, tree_instance* ti, base_node* const child) {
        std::size_t cnk = get_permutation_cnk();
        for (std::size_t i = 0; i < cnk; ++i) {
            std::size_t index = permutation_.get_index_of_rank(i);
            if (child == lv_.at(index).get_next_layer()) {
                delete_of<false>(token, ti, get_key_slice_at(index),
                                 get_key_length_at(index)); // NOLINT
                return;
            }
        }
        // unreachable points.
        LOG(ERROR) << log_location_prefix;
    }

    /**
      * @brief release all heap objects and clean up.
      */
    status destroy() override {
        std::size_t cnk = get_permutation_cnk();
        std::vector<std::thread> th_vc;
        for (std::size_t i = 0; i < cnk; ++i) {
            // living link or value
            std::size_t index = permutation_.get_index_of_rank(i);
            // cleanup process
            auto process = [this](std::size_t i) { lv_.at(i).destroy(); };
            if (lv_.at(index).get_next_layer() != nullptr) {
                // has some layer, considering parallel
                if (destroy_manager::check_room()) {
                    th_vc.emplace_back(process, index);
                } else {
                    process(index);
                }
            } else {
                // not some layer, not considering parallel
                process(index);
            }
        }
        for (auto&& th : th_vc) {
            th.join();
            destroy_manager::return_room();
        }

        return status::OK_DESTROY_BORDER;
    }

    /**
      * @pre
      * This border node was already locked by caller.
      * This function is called by remove func.
      * The key-value corresponding to @a key_slice and @a key_length exists in this node.
      * @details delete value corresponding to @a key_slice and @a key_length
      * @param[in] token
      * @param[in] key_slice The key slice of key-value.
      * @param[in] key_slice_length The @a key_slice length.
      * @param[in] target_is_value
      */
    template<bool target_is_value>
    void delete_of(Token token, tree_instance* ti,
                   const key_slice_type key_slice,
                   const key_length_type key_slice_length) {
        // past: delete is treated to increment vinsert counter.
        //set_version_inserting_deleting(true);
        /**
          * find position.
          */
        std::size_t cnk = get_permutation_cnk();
        for (std::size_t i = 0; i < cnk; ++i) {
            std::size_t index = permutation_.get_index_of_rank(i);
            if ((key_slice_length == 0 && get_key_length_at(index) == 0) ||
                (key_slice_length == get_key_length_at(index) &&
                 memcmp(&key_slice, &get_key_slice_ref().at(index),
                        sizeof(key_slice_type)) == 0)) {
                delete_at(token, i, index, target_is_value);
                if (cnk == 1) { // attention : this cnk is before delete_at;
                    set_version_deleted(true);
                    if (ti->load_root_ptr() != this) {
                        // root && deleted node is treated as special. This isn't.
                        set_version_root(false);
                    }

                    /**
                      * After this delete operation, this border node is empty.
                      */
                retry_prev_lock:
                    border_node* prev = get_prev();
                    if (prev != nullptr) {
                        prev->lock();
                        if (prev->get_version_deleted() || prev != get_prev()) {
                            prev->version_unlock();
                            goto retry_prev_lock; // NOLINT
                        } else {
                            prev->set_next(get_next());
                            if (get_next() != nullptr) {
                                get_next()->set_prev(prev);
                            }
                            prev->version_unlock();
                        }
                    } else if (get_next() != nullptr) {
                        get_next()->set_prev(nullptr);
                    }
                    /**
                      * lock order is next to prev and lower to higher.
                      */
                    base_node* pn = lock_parent();
                    if (pn == nullptr) {
                        //ti->store_root_ptr(nullptr);
                        // remain empty deleted root node.
                        version_unlock();
                        return;
                    }
                    /**
                      * This node has parent node, so this must not be root.
                      * note: Including relation of parent-child is
                      * border-border.
                      */
                    set_version_root(false);
                    version_unlock();
                    if (pn->get_version_border()) {
                        dynamic_cast<border_node*>(pn)->delete_of(token, ti,
                                                                  this);
                    } else {
                        dynamic_cast<interior_node*>(pn)
                                ->delete_of<border_node>(token, ti, this);
                    }
                    auto* tinfo =
                            reinterpret_cast<thread_info*>(token); // NOLINT
                    tinfo->get_gc_info().push_node_container(
                            {tinfo->get_begin_epoch(), this});
                } else {
                    version_unlock();
                }
                return;
            }
        }
        /**
          * unreachable.
          */
        LOG(ERROR) << log_location_prefix
                   << ", deleted: " << get_version_deleted()
                   << ", is root: " << get_version_root();
    }

    /**
      * @details display function for analysis and debug.
      */
    void display() override {
        display_base();
        cout << "border_node::display" << endl;
        permutation_.display();
        for (std::size_t i = 0; i < get_permutation_cnk(); ++i) {
            lv_.at(i).display();
        }
        cout << "next : " << get_next() << endl;
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

        const std::size_t cnk = get_permutation_cnk();
        ++node_num;
        reserved += sizeof(border_node);
        used += sizeof(border_node) -
                ((key_slice_length - cnk) * sizeof(link_or_value));

        for (std::size_t i = 0; i < cnk; ++i) {
            std::size_t index = permutation_.get_index_of_rank(i);
            lv_.at(index).mem_usage(level, mem_stat);
        }
    }

    /**
      * @post It is necessary for the caller to verify whether the extraction is appropriate.
      * @param[out] next_layers
      * @attention layers are stored in ascending order.
      * @return
      */
    [[maybe_unused]] void
    get_all_next_layer(std::vector<base_node*>& next_layers) {
        next_layers.clear();
        std::size_t cnk = permutation_.get_cnk();
        for (std::size_t i = 0; i < cnk; ++i) {
            link_or_value* lv = get_lv_at(permutation_.get_index_of_rank(i));
            base_node* nl = lv->get_next_layer();
            if (nl != nullptr) { next_layers.emplace_back(nl); }
        }
    }

    [[maybe_unused]] [[nodiscard]] std::array<link_or_value, key_slice_length>&
    get_lv() {
        return lv_;
    }

    /**
      * @details Find link_or_value element whose next_layer is the same as @a next_layer of
      * the argument.
      * @pre Executor has lock of this node. There is always a lv that points to a pointer
      * given as an argument.
      * @param[in] next_layer
      * @return link_or_value*
      */
    [[maybe_unused]] [[nodiscard]] link_or_value*
    get_lv(base_node* const next_layer) {
        for (std::size_t i = 0; i < key_slice_length; ++i) {
            if (lv_.at(i).get_next_layer() == next_layer) { return &lv_.at(i); }
        }
        /**
          * unreachable point.
          */
        LOG(ERROR) << log_location_prefix;
        return nullptr;
    }

    [[nodiscard]] link_or_value* get_lv_at(const std::size_t index) {
        return &lv_.at(index);
    }

    /**
     * @brief
     *
     * @param key_slice
     * @param key_length
     * @pre Caller (put) must lock this node.
     * @return std::size_t
     */
    std::size_t compute_rank_if_insert(const key_slice_type key_slice,
                                       const key_length_type key_length) {
        permutation perm{permutation_.get_body()};
        std::size_t cnk = perm.get_cnk();
        for (std::size_t i = 0; i < cnk; ++i) {
            std::size_t index = perm.get_index_of_rank(i);
            key_slice_type target_key_slice = get_key_slice_at(index);
            key_length_type target_key_len = get_key_length_at(index);
            if (key_length == 0 && target_key_len == 0) {
                LOG(ERROR) << log_location_prefix << "unexpected path";
                return 0;
            }
            // not zero key
            auto ret = memcmp(&key_slice, &target_key_slice,
                              sizeof(key_slice_type));
            if (ret == 0) {
                if ((key_length > sizeof(key_slice_type) &&
                     target_key_len > sizeof(key_slice_type)) ||
                    key_length == target_key_len) {
                    LOG(ERROR) << log_location_prefix << "unexpected path";
                    return 0;
                }
                if (key_length < target_key_len) { return i; }
            } else if (ret < 0) {
                return i;
                break;
            }
        }

        return cnk;
    }

    /**
      * @attention This function must not be called with locking of this node. Because
      * this function executes get_stable_version and it waits own (lock-holder)
      * infinitely.
      * @param[in] key_slice
      * @param[in] key_length
      * @param[out] stable_v  the stable version which is at atomically fetching lv.
      * @param[out] lv_pos
      * @return
      */
    [[nodiscard]] link_or_value* get_lv_of(const key_slice_type key_slice,
                                           const key_length_type key_length,
                                           node_version64_body& stable_v,
                                           std::size_t& lv_pos) {
        node_version64_body v = get_stable_version();
        for (;;) {
            /**
       * It loads cnk atomically by get_cnk func.
       */
            permutation perm{permutation_.get_body()};
            std::size_t cnk = perm.get_cnk();
            link_or_value* ret_lv{nullptr};
            for (std::size_t i = 0; i < cnk; ++i) {
                bool suc{false};
                std::size_t index = perm.get_index_of_rank(i);
                key_slice_type target_key_slice = get_key_slice_at(index);
                key_length_type target_key_len = get_key_length_at(index);
                if (key_length == 0 && target_key_len == 0) {
                    suc = true;
                } else {
                    auto ret = memcmp(&key_slice, &target_key_slice,
                                      sizeof(key_slice_type));
                    if (ret == 0) {
                        if ((key_length > sizeof(key_slice_type) &&
                             target_key_len > sizeof(key_slice_type)) ||
                            key_length == target_key_len) {
                            suc = true;
                        } else if (key_length < target_key_len) {
                            break;
                        }
                    } else if (ret < 0) {
                        break;
                    }
                }

                if (suc) {
                    ret_lv = get_lv_at(index);
                    lv_pos = index;
                    break;
                }
            }
            node_version64_body v_check = get_stable_version();
            if (v == v_check) {
                stable_v = v;
                return ret_lv;
            }
            v = v_check;
        }
    }

    [[nodiscard]] link_or_value*
    get_lv_of_without_lock(const key_slice_type key_slice,
                           const key_length_type key_length) {
        /**
          * It loads cnk atomically by get_cnk func.
          */
        permutation perm{permutation_.get_body()};
        std::size_t cnk = perm.get_cnk();
        link_or_value* ret_lv{nullptr};
        for (std::size_t i = 0; i < cnk; ++i) {
            bool suc{false};
            std::size_t index = perm.get_index_of_rank(i);
            key_slice_type target_key_slice = get_key_slice_at(index);
            key_length_type target_key_len = get_key_length_at(index);
            if (key_length == 0 && target_key_len == 0) {
                suc = true;
            } else {
                auto ret = memcmp(&key_slice, &target_key_slice,
                                  sizeof(key_slice_type));
                if (ret == 0) {
                    if ((key_length > sizeof(key_slice_type) &&
                         target_key_len > sizeof(key_slice_type)) ||
                        key_length == target_key_len) {
                        suc = true;
                    } else if (key_length < target_key_len) {
                        break;
                    }
                } else if (ret < 0) {
                    break;
                }
            }

            if (suc) { ret_lv = get_lv_at(index); }
        }
        return ret_lv;
    }

    border_node* get_next() { return loadAcquireN(next_); }

    permutation& get_permutation() { return permutation_; }

    [[nodiscard]] std::uint8_t get_permutation_cnk() const {
        return permutation_.get_cnk();
    }

    [[maybe_unused]] [[nodiscard]] std::size_t
    get_permutation_lowest_key_pos() const {
        return permutation_.get_lowest_key_pos();
    }

    border_node* get_prev() { return loadAcquireN(prev_); }

    void init_border() {
        init_base();
        init_border_member_range(0);
        set_version_root(true);
        set_version_border(true);
        permutation_.init();
        set_next(nullptr);
        set_prev(nullptr);
    }

    /**
      * @details init at @a pos as position.
      * @param[in] pos This is a position (index) to be initialized.
      */
    void init_border(const std::size_t pos) {
        init_base(pos);
        lv_.at(pos).init_lv();
    }

    /**
      * @pre This function is called by put function.
      * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
      * @pre This function can not be called for updating existing nodes.
      * @pre If this function is used for node creation, link after set because
      * set function does not execute lock function.
      * @details This function inits border node by using arguments.
      * @param[in] key_view
      * @param[in] new_value
      * @param[out] created_value_ptr The pointer to created value in yakushima.
      * @param[in] root is the root node of the layer.
      */
    template<class ValueType>
    void init_border(std::string_view key_view, value* new_value,
                     ValueType** const created_value_ptr, const bool root) {
        init_border();
        set_version_root(root);
        get_version_ptr()->atomic_inc_vinsert();
        insert_lv_at(get_permutation().get_empty_slot(), key_view, new_value,
                     reinterpret_cast<void**>(created_value_ptr), // NOLINT
                     0);
    }

    void init_border_member_range(const std::size_t start) {
        for (auto i = start; i < lv_.size(); ++i) { lv_.at(i).init_lv(); }
    }

    /**
      * @pre It already locked this node.
      * @param[in] index
      * @param[in] key_view
      * @param[in] value_ptr
      * @param[out] created_value_ptr
      * @param[in] arg_value_length
      * @param[in] value_align
      * @param[in] rank
      */
    void insert_lv_at(const std::size_t index, std::string_view key_view,
                      value* new_value, void** const created_value_ptr,
                      const std::size_t rank) {
        /**
          * attention: key_slice must be initialized to 0.
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
            /**
              * You only need to know that it is 8 bytes or more. If it is
              * stored obediently, key_length_type must be a large size type.
              */
            set_key_length_at(index, sizeof(key_slice_type) + 1);
            border_node* next_layer_border = new border_node(); // NOLINT
            key_view.remove_prefix(sizeof(key_slice_type));
            /**
              * attention: next_layer_border is the root of next layer.
              */
            next_layer_border->init_border(key_view, new_value,
                                           created_value_ptr, true);
            next_layer_border->set_parent(this);
            set_lv_next_layer(index, next_layer_border);
        } else {
            memcpy(&key_slice, key_view.data(), key_view.size());
            set_key_slice_at(index, key_slice);
            set_key_length_at(index,
                              static_cast<key_length_type>(key_view.size()));
            set_lv_value(index, new_value, created_value_ptr);
        }
        permutation_.inc_key_num();
        permutation_.insert(rank, index);
    }

    void permutation_rearrange() {
        permutation_.rearrange(get_key_slice_ref(), get_key_length_ref());
    }

    [[maybe_unused]] void set_permutation_cnk(const std::uint8_t n) {
        permutation_.set_cnk(n);
    }

    /**
      * @pre This is called by split process.
      * @param index
      * @param nlv
      */
    void set_lv(const std::size_t index, link_or_value* const nlv) {
        lv_.at(index).set(nlv);
    }

    /**
      * @brief set value to link_or_value.
      * @param[in] index todo write
      * @param[in] new_value todo write
      * @param[out] created_value_ptr todo write
      */
    void set_lv_value(const std::size_t index, value* new_value,
                      void** const created_value_ptr) {
        lv_.at(index).set_value(new_value, created_value_ptr);
    }

    void set_lv_next_layer(const std::size_t index,
                           base_node* const next_layer) {
        lv_.at(index).set_next_layer(next_layer);
    }

    void set_next(border_node* const nnext) { storeReleaseN(next_, nnext); }

    void set_prev(border_node* const prev) { storeReleaseN(prev_, prev); }

    void shift_left_border_member(const std::size_t start_pos,
                                  const std::size_t shift_size) {
        memmove(get_lv_at(start_pos - shift_size), get_lv_at(start_pos),
                sizeof(link_or_value) * (key_slice_length - start_pos));
    }

private:
    // first member of base_node is aligned along with cache line size.
    /**
     * @attention This variable is read/written concurrently.
     */
    permutation permutation_{};
    /**
     * @attention This variable is read/written concurrently.
     */
    std::array<link_or_value, key_slice_length> lv_{};
    /**
     * @attention This is protected by its previous sibling's lock.
     */
    border_node* prev_{nullptr};
    /**
     * @attention This variable is read/written concurrently.
     */
    border_node* next_{nullptr};
};
} // namespace yakushima
