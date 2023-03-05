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
    static value* create_value(const void* in_ptr, value_length_type v_len,
                               value_align_type v_align) {
        // compute the size/alignment to be reserved
        constexpr auto kMinAlignment = static_cast<value_align_type>(8);
        if (v_align < kMinAlignment) { v_align = kMinAlignment; }
        const auto total_len = v_len + static_cast<value_length_type>(v_align);

        // allocate memory and copy the given value
        // NOTE: it use copy assign, so ValueType must be copy-assignable.
        value* v{};
        try {
            auto* page = ::operator new(total_len, v_align);
            v = new (page) value{v_len, v_align}; // NOLINT
        } catch (std::bad_alloc& e) {
            LOG(ERROR) << log_location_prefix << e.what();
        }
        memcpy(v->get_body(), in_ptr, v_len);

        return v;
    }

    /**
     * @brief Release the given value pointer.
     * 
     * This function computes the actual size and alignment of the given value to
     * release the allocated memory correctly.
     * 
     * @param[in] v The value pointer to be deleted.
     */
    static void delete_value(value* v) {
        ::operator delete(v, v->get_total_len(), v->get_align());
    }

    /**
     * @return The address of the contained value.
     */
    [[nodiscard]] void* get_body() {
        return reinterpret_cast<void*>(                         // NOLINT
                &(reinterpret_cast<std::byte*>(this)[align_])); // NOLINT
    }

    /**
     * @return The length of the contained value.
     */
    [[nodiscard]] value_length_type get_len() const { return len_; }

    /**
     * @return The allocated memory size for this value.
     */
    [[nodiscard]] value_length_type get_total_len() const {
        return len_ + align_;
    }

    /**
     * @return The alignment size of the contained value.
     */
    [[nodiscard]] value_align_type get_align() const {
        return static_cast<value_align_type>(align_);
    }

    /**
     * @retval true The contained value to be deleted.
     * @retval false  Otherwise.
     */
    [[nodiscard]] bool need_delete() const { return need_delete_; }

    /**
     * @brief Set the flag for deletion off.
     * 
     */
    void remove_delete_flag() { need_delete_ = false; }

private:
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