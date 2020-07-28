/**
 * @file link_or_value.h
 */

#pragma once

#include "atomic_wrapper.h"
#include "base_node.h"
#include "cpu.h"

#include <typeinfo>

namespace yakushima {

class link_or_value {
public:
  /**
   * These are mainly used at link_or_value.h
   * To avoid circular reference at there, declare originals at base_node.h.
   */
  using value_align_type = base_node::value_align_type;
  using value_length_type = base_node::value_length_type;

  link_or_value() = default;

  link_or_value(const link_or_value &) = default;

  link_or_value(link_or_value &&) = default;

  link_or_value &operator=(const link_or_value &) = default;

  link_or_value &operator=(link_or_value &&) = default;

  ~link_or_value() = default;

  /**
   * @details release heap objects.
   */
  void destroy() {
    destroy_next_layer();
    destroy_value();
  }

  void destroy_next_layer() {
    if (next_layer_ != nullptr) {
      next_layer_->destroy();
      delete next_layer_; // NOLINT
    }
    set_next_layer(nullptr);
  }

  void destroy_value() {
    if (get_need_delete_value()) {
      ::operator delete(v_or_vp_, static_cast<std::align_val_t>((get_value_align())));
    }
    set_need_delete_value(false);
    set_v_or_vp(nullptr);
    set_value_length(0);
    set_value_align(0);
  }

  /**
   * @details display function for analysis and debug.
   */
  void display() {
    std::cout << "need_delete_value_ : " << get_need_delete_value() << std::endl;
    std::cout << "next_layer_ : " << get_next_layer() << std::endl;
    std::cout << "v_or_vp_ : " << get_v_or_vp_() << std::endl;
    std::cout << "value_length_ : " << get_value_length() << std::endl;
    std::cout << "value_align_ : " << get_value_align() << std::endl;
  }

  [[nodiscard]] bool get_need_delete_value() {
    return loadAcquireN(need_delete_value_);
  }

  [[nodiscard]] base_node *get_next_layer() {
    return loadAcquireN(next_layer_);
  }

  [[maybe_unused]] [[nodiscard]] const std::type_info *get_lv_type() {
    if (get_next_layer() != nullptr) {
      return &typeid(get_next_layer());
    }
    if (get_v_or_vp_() != nullptr) {
      return &typeid(get_v_or_vp_());
    }
    return &typeid(nullptr);
  }

  [[nodiscard]] void *get_v_or_vp_() {
    return loadAcquireN(v_or_vp_);
  }

  [[nodiscard]] value_align_type get_value_align() {
    return loadAcquireN(value_align_);
  }

  [[nodiscard]] value_length_type get_value_length() {
    return loadAcquireN(value_length_);
  }

  void init_lv() {
    set_need_delete_value(false);
    set_next_layer(nullptr);
    set_v_or_vp(nullptr);
    set_value_length(0);
    set_value_align(0);
  }

  /**
   * @pre This is called by split process.
   * @details This is move process.
   * @param nlv
   */
  void set(link_or_value *nlv) {
    /**
     * This object in this function is not accessed concurrently, so it can copy assign.
     */
    *this = *nlv;
  }

  /**
   * @pre @a arg_value_length is divisible by sizeof( @a ValueType ).
   * @pre This function called at initialization.
   * @param vptr The pointer to source value object.
   * @param arg_value_size The byte size of value.
   * @param value_align The alignment of value.
   */
  void set_value(void *vptr, std::size_t arg_value_size,
                 std::size_t value_align) {
    if (get_need_delete_value()) {
      ::operator delete(get_v_or_vp_(), static_cast<std::align_val_t>(get_value_align()));
      set_need_delete_value(false);
    }
    set_next_layer(nullptr);
    set_value_length(arg_value_size);
    set_value_align(value_align);
    try {
      /**
       * It use copy assign, so ValueType must be copy-assignable.
       */
      set_v_or_vp(::operator new(arg_value_size, static_cast<std::align_val_t>(value_align)));
      memcpy(get_v_or_vp_(), vptr, arg_value_size);
      set_need_delete_value(true);
    } catch (std::bad_alloc &e) {
      std::cout << e.what() << std::endl;
      std::cerr << __FILE__ << ": " << __LINE__ << std::endl;
      std::abort();
    }
  }

  /**
   * @pre This function called at initialization.
   * @param[in] new_next_layer
   */
  void set_next_layer(base_node *new_next_layer) {
    storeReleaseN(next_layer_, new_next_layer);
  }

  void set_v_or_vp(void *new_v_or_vp) {
    storeReleaseN(v_or_vp_, new_v_or_vp);
  }

  void set_value_align(value_align_type new_value_align) {
    storeReleaseN(value_align_, new_value_align);
  }

  void set_value_length(value_length_type new_value_length) {
    storeReleaseN(value_length_, new_value_length);
  }

  void set_need_delete_value(bool new_need_delete_value) {
    storeReleaseN(need_delete_value_, new_need_delete_value);
  }

private:
  /**
   * @attention
   * This variable is read/write concurrently.
   * If this is nullptr, value is stored.
   * If this is not nullptr, it contains next_layer.
   */
  base_node *next_layer_{nullptr};
  /**
   * @details
   * This variable is stored value body whose size is less than pointer or pointer to value.
   * This variable is read/write concurrently.
   */
  void *v_or_vp_{nullptr};
  /**
   * @attention
   * This variable is read/write concurrently.
   * This variable is updated at initialization and destruction.
   */
  value_length_type value_length_{0};
  value_align_type value_align_{0};
  /**
   * @attention
   * This variable is read/write concurrently.
   * This variable is updated at initialization and destruction.
   */
  bool need_delete_value_{false};
};

} // namespace yakushima
