/**
 * @file base_node.h
 */

#pragma once

#include <array>
#include <cstring>
#include <functional>

#include "atomic_wrapper.h"
#include "cpu.h"
#include "scheme.h"
#include "version.h"

#include "glog/logging.h"

namespace yakushima {

class base_node { // NOLINT
public:
    class key_tuple {
    public:
        key_tuple() = default;

        key_tuple(key_slice_type slice, key_length_type length)
            : key_slice_(slice), key_length_(length) {}

        bool operator<(const key_tuple& r) const {
            if (key_length_ == 0) { return true; }
            if (r.key_length_ == 0) { return false; }
            int ret = memcmp(&key_slice_, &r.key_slice_,
                             key_length_ < r.key_length_ ? key_length_
                                                         : r.key_length_);
            if (ret < 0) { return true; }
            if (ret == 0) { return key_length_ < r.key_length_; }
            return false;
        }

        bool operator==(const key_tuple& r) const {
            return key_slice_ == r.key_slice_ && key_length_ == r.key_length_;
        }

        [[nodiscard]] key_length_type get_key_length() const {
            return key_length_;
        }

        [[nodiscard]] key_slice_type get_key_slice() const {
            return key_slice_;
        }

        void set_key_length(const key_length_type length) {
            key_length_ = length;
        }

        void set_key_slice(const key_slice_type slice) { key_slice_ = slice; }

    private:
        key_slice_type key_slice_{0};
        key_length_type key_length_{0};
    };

    virtual ~base_node() = default; // NOLINT

    void atomic_set_version_root(const bool tf) {
        version_.atomic_set_root(tf);
    }

    /**
   * A virtual function is defined because It wants to distinguish the children class of
   * the contents by using polymorphism. So this function is pure virtual function.
   */
    virtual status destroy() = 0;

    /**
   * @details display function for analysis and debug.
   */
    virtual void display() = 0;

    void display_base() {
        std::cout << "base_node::display_base" << std::endl;
        version_.display();
        std::cout << "parent_ : " << get_parent() << std::endl;
        for (std::size_t i = 0; i < key_slice_length; ++i) {
            std::cout << "key_slice_[" << i
                      << "] : " << std::to_string(get_key_slice_at(i))
                      << std::endl;
            std::cout << "key_length_[" << i
                      << "] : " << std::to_string(get_key_length_at(i))
                      << std::endl;
        }
    }

    [[nodiscard]] const std::array<key_length_type, key_slice_length>&
    get_key_length_ref() const {
        return key_length_;
    }

    [[nodiscard]] key_length_type
    get_key_length_at(const std::size_t index) const {
        return key_length_.at(index);
    }

    [[nodiscard]] const std::array<key_slice_type, key_slice_length>&
    get_key_slice_ref() const {
        return key_slice_;
    }

    [[nodiscard]] key_slice_type
    get_key_slice_at(const std::size_t index) const {
        return key_slice_.at(index);
    }

    [[maybe_unused]] [[nodiscard]] bool get_lock() const {
        return version_.get_locked();
    }

    [[nodiscard]] node_version64_body get_stable_version() const {
        return version_.get_stable_version();
    }

    [[nodiscard]] base_node* get_parent() const {
        return loadAcquireN(parent_);
    }

    [[nodiscard]] node_version64_body get_version() const {
        return version_.get_body();
    }

    [[nodiscard]] node_version64* get_version_ptr() { return &version_; }

    [[nodiscard]] bool get_version_border() const {
        return version_.get_border();
    }

    [[nodiscard]] bool get_version_deleted() const {
        return version_.get_deleted();
    }

    [[nodiscard]] bool get_version_root() const { return version_.get_root(); }

    [[nodiscard]] node_version64_body::vinsert_delete_type
    get_version_vinsert_delete() const {
        return version_.get_vinsert_delete();
    }

    [[nodiscard]] node_version64_body::vsplit_type get_version_vsplit() const {
        return version_.get_vsplit();
    }

    void init_base() {
        version_.init();
        set_parent(nullptr);
        key_slice_.fill(0);
        key_length_.fill(0);
    }

    /**
   * @details init at @a pos as position.
   * @param[in] pos This is a position (index) to be initialized.
   */
    void init_base(const std::size_t pos) { set_key(pos, 0, 0); }

    [[maybe_unused]] void init_base_member_range(const std::size_t start) {
        for (std::size_t i = start; i < key_slice_length; ++i) {
            set_key(i, 0, 0);
        }
    }

    /**
   * @brief It locks this node.
   * @pre It didn't lock by myself.
   * @return void
   */
    void lock() { version_.lock(); }

    /**
   * @pre This function is called by split.
   */
    [[nodiscard]] base_node* lock_parent() const {
        base_node* p = get_parent();
        for (;;) {
            if (p == nullptr) { return nullptr; }
            p->lock();
            base_node* check = get_parent();
            if (p == check) { return p; }
            p->version_unlock();
            p = check;
        }
    }

    [[maybe_unused]] void move_key_to_base_range(base_node* const right,
                                                 const std::size_t start) {
        for (auto i = start; i < key_slice_length; ++i) {
            right->set_key(i - start, get_key_slice_at(i),
                           get_key_length_at(i));
            set_key(i, 0, 0);
        }
    }

    void set_key(const std::size_t index, const key_slice_type key_slice,
                 const key_length_type key_length) {
        set_key_slice_at(index, key_slice);
        set_key_length_at(index, key_length);
    }

    void set_key_length_at(const std::size_t index,
                           const key_length_type length) {
        storeReleaseN(key_length_.at(index), length);
    }

    void set_key_slice_at(const std::size_t index,
                          const key_slice_type key_slice) {
        storeReleaseN(key_slice_.at(index), key_slice);
    }

    void set_parent(base_node* const new_parent) {
        storeReleaseN(parent_, new_parent);
    }

    [[maybe_unused]] void
    set_version(const node_version64_body nv) { // this function is used.
        version_.set_body(nv);
    }

    void set_version_border(const bool tf) { version_.atomic_set_border(tf); }

    void set_version_deleted(const bool tf) { version_.atomic_set_deleted(tf); }

    void set_version_inserting_deleting(const bool tf) {
        version_.atomic_set_inserting_deleting(tf);
    }

    void set_version_root(const bool tf) { version_.atomic_set_root(tf); }

    [[maybe_unused]] void set_version_splitting(const bool tf) {
        version_.atomic_set_splitting(tf);
    }

    void shift_left_base_member(const std::size_t start_pos,
                                const std::size_t shift_size) {
        memmove(&key_slice_.at(start_pos - shift_size),
                &key_slice_.at(start_pos),
                sizeof(key_slice_type) * (key_slice_length - start_pos));
        memmove(&key_length_.at(start_pos - shift_size),
                &key_length_.at(start_pos),
                sizeof(key_length_type) * (key_slice_length - start_pos));
    }

    void shift_right_base_member(const std::size_t start,
                                 const std::size_t shift_size) {
        memmove(&key_slice_.at(start + shift_size), &key_slice_.at(start),
                sizeof(key_slice_type) *
                        (key_slice_length - start - shift_size));
        memmove(&key_length_.at(start + shift_size), &key_length_.at(start),
                sizeof(key_length_type) *
                        (key_slice_length - start - shift_size));
    }

    /**
   * @brief It unlocks this node.
   * @pre This node was already locked.
   * @return void
   */
    void version_unlock() { version_.unlock(); }

    [[maybe_unused]] void version_atomic_inc_vinsert() {
        version_.atomic_inc_vinsert();
    }

private:
    /**
     * @attention This variable is read/written concurrently.
     */
    std::array<key_slice_type, key_slice_length> key_slice_{};
    /**
     * @attention This member is protected by its parent's lock.
     * In the original paper, Fig 2 tells that parent's type is interior_node*,
     * however, at Fig 1, parent's type is interior_node or border_node both
     * interior's view and border's view.
     * This variable is read/written concurrently.
     */
    base_node* parent_{nullptr};
    /**
     * @attention This variable is read/written concurrently.
     */
    node_version64 version_{};
    /**
     * @attention This variable is read/written concurrently.
     * @details This is used for distinguishing the identity of link or value and same
     * slices. For example, key 1 : \0, key 2 : \0\0, ... , key 8 : \0\0\0\0\0\0\0\0. These
     * keys have same key_slices (0) but different key_length. If the length is more than 8,
     * the lv points out to next layer.
     */
    std::array<key_length_type, key_slice_length> key_length_{};
};

} // namespace yakushima