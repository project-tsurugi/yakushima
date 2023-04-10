/**
 * @file value.h
 */

#include "atomic_wrapper.h"
#include "scheme.h"

namespace yakushima {

class value {
public:
    /**
     * @brief Create a new value instance with dynamic memory allocation.
     * 
     * @param[in] in_ptr The source address of a new value.
     * @param[in] v_len The length of a new value.
     * @param[in] v_align The alignment size of a new value.
     * @return The pointer to the new value.
     */
    template<bool kIsInlineValue>
    [[nodiscard]] static value* create_value(const void* in_ptr,
                                             value_length_type v_len,
                                             value_align_type v_align) {
        value* v{};
        if constexpr (kIsInlineValue) {
            memcpy(&v, in_ptr, sizeof(uintptr_t));
        } else {
            // compute the size/alignment to be reserved
            constexpr auto kMinAlignment = static_cast<value_align_type>(8);
            if (v_align < kMinAlignment) { v_align = kMinAlignment; }
            const auto total_len =
                    v_len + static_cast<value_length_type>(v_align);

            // allocate memory and copy the given value
            // NOTE: it use copy assign, so ValueType must be copy-assignable.
            try {
                auto* page = ::operator new(total_len, v_align);
                v = new (page) value{v_len, v_align}; // NOLINT
            } catch (std::bad_alloc& e) {
                LOG(ERROR) << log_location_prefix << e.what();
            }
            auto ptr = reinterpret_cast<uintptr_t>(v) | kValPtrFlag; // NOLINT
            v = reinterpret_cast<value*>(ptr);                       // NOLINT

            memcpy(value::get_body(v), in_ptr, v_len);
        }
        return v;
    }

    /**
     * @brief Release the given value pointer.
     * 
     * This function computes the actual size and alignment of the given value to
     * release the allocated memory correctly.
     * 
     * @param[in] val The value pointer to be deleted.
     */
    static void delete_value(value* val) {
        auto* v = remove_ptr_flag(val);
        if (v != val) {
            const auto v_align = static_cast<value_align_type>(v->align_);
            ::operator delete(v, v->len_ + v->align_, v_align);
        }
    }

    /**
     * @param[in] val The target value pointer.
     * @retval true if the region of the given value is dynamically allocated.
     * @retval false if the given value is embedded as inline.
     */
    static bool is_value_ptr(const value* val) {
        return (reinterpret_cast<uintptr_t>(val) & kValPtrFlag) > 0; // NOLINT
    }

    /**
     * @param[in] val The target value pointer.
     * @retval true if the contained value should be deleted.
     * @retval false otherwise.
     */
    static bool need_delete(const value* val) {
        auto* v = remove_ptr_flag(val);
        if (v == val) { return false; }
        return v->need_delete_;
    }

    /**
     * @param[in] val The target value pointer.
     * @return The address of the contained value.
     */
    static void* get_body(value* val) {
        auto* v = remove_ptr_flag(val);
        if (v == val) { return v; }
        return reinterpret_cast<void*>(                         // NOLINT
                &(reinterpret_cast<std::byte*>(v)[v->align_])); // NOLINT
    }

    /**
     * @param[in] val The target value pointer.
     * @return The length of the contained value.
     */
    static value_length_type get_len(const value* val) {
        auto* v = remove_ptr_flag(val);
        if (v == val) { return sizeof(uintptr_t); }
        return v->len_;
    }

    /**
     * @param[in] val The target value pointer.
     * @retval 1st: The address of the given value.
     * @retval 2nd: The allocated memory size for the given value.
     * @retval 3rd: The alignment size of the given value.
     */
    static std::tuple<void*, value_length_type, value_align_type>
    get_gc_info(const value* val) {
        auto* v = remove_ptr_flag(val);
        if (v == val) { return {nullptr, 0, static_cast<value_align_type>(0)}; }
        return {v, v->len_ + v->align_,
                static_cast<value_align_type>(v->align_)};
    }

    /**
     * @brief Set the flag for deletion off.
     * 
     * @param[in] val The target value pointer.
     */
    static void remove_delete_flag(value* val) {
        auto* v = remove_ptr_flag(val);
        if (v != val) { v->need_delete_ = false; }
    }

private:
    /**
     * @brief A flag for indicating that the next layer exists.
     * 
     */
    static constexpr uintptr_t kValPtrFlag = 0b01UL << 62UL;

    /**
     * @brief Internal constructor for setting header information.
     * 
     * @param[in] v_len The length of a new value.
     * @param[in] v_align The alignment size of a new value.
     */
    value(value_length_type v_len, value_align_type v_align)
        : len_(v_len),
          align_(static_cast<std::uint16_t>(v_align)), need_delete_{true} {}

    /**
     * @param[in] val The target value pointer.
     * @return The actual pointer without a flag.
     */
    static value* remove_ptr_flag(const value* val) {
        return reinterpret_cast<value*>(                          // NOLINT
                reinterpret_cast<uintptr_t>(val) & ~kValPtrFlag); // NOLINT
    }

    /**
     * @brief The length of the contained value.
     */
    std::uint32_t len_{0};

    /**
     * @brief The alignment size of the contained value.
     * 
     * Note that the minimum alignment is 8 bytes to retain the header region.
     */
    std::uint16_t align_{0};

    /**
     * @brief A flag for indicating this value is active.
     */
    bool need_delete_{false};
};

} // namespace yakushima