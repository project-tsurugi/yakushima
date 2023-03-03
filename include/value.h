/**
 * @file value.h
 */

#include "atomic_wrapper.h"
#include "scheme.h"

namespace yakushima {

class value {
public:
    value() = default;

    value(void* body, value_length_type len, value_align_type align)
        : body_(body), len_(len), align_(static_cast<std::uint16_t>(align)) {}

    [[nodiscard]] value_align_type get_align() const {
        return static_cast<value_align_type>(align_);
    }

    [[nodiscard]] void* get_body() const { return body_; }

    [[nodiscard]] value_length_type get_len() const { return len_; }

    [[nodiscard]] bool get_need_delete_value() const { return need_delete_; }

    void set_align(value_align_type const v_align) {
        align_ = static_cast<std::uint16_t>(v_align);
    }

    void set_body(void* const body) { body_ = body; }

    void set_len(value_length_type const v_len) { len_ = v_len; }

    void set_need_delete(const bool tf) { need_delete_ = tf; }

private:
    /**
     * @details
     * This variable is stored value body whose size is less than pointer or pointer to
     * value. This variable is read/write concurrently.
     */
    void* body_{nullptr};

    /**
     * @attention
     * This variable is read/write concurrently.
     * This variable is updated at initialization and destruction.
     */
    std::uint32_t len_{0};

    std::uint16_t align_{0};

    /**
     * @attention
     * This variable is read/write concurrently.
     * This variable is updated at initialization and destruction.
     */
    bool need_delete_{false};
};

} // namespace yakushima