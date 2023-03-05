/**
 * @file border_node_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"

using namespace yakushima;

namespace yakushima::testing {

class bnt : public ::testing::Test {};

TEST_F(bnt, alignment) { // NOLINT
    ASSERT_EQ(alignof(border_node), CACHE_LINE_SIZE);
}

TEST_F(bnt, display) { // NOLINT
    border_node bn;
    ASSERT_EQ(true, true);
    // bn.display();
    bn.init_border();
    ASSERT_EQ(true, true);
    // bn.display();
}

TEST_F(bnt, shift_left_border_member) { // NOLINT
    constexpr auto kUseDynamicAllocation = false;
    constexpr auto kVAlign =
            static_cast<value_align_type>(alignof(std::size_t));

    border_node bn;
    for (std::size_t i = 0; i < 15; ++i) {
        auto* v = value::create_value<kUseDynamicAllocation>(&i, i, kVAlign);
        bn.set_lv_value(i, v, nullptr);
    }
    for (std::size_t i = 0; i < 5; ++i) {
        const auto* leftmost_lv = bn.get_lv_at(0);
        const auto* lm_v = leftmost_lv->get_value();

        bn.shift_left_border_member(1, 1);

        const auto* moved_v = bn.get_lv_at(0)->get_value();
        ASSERT_EQ(value::get_len(lm_v) + 1, value::get_len(moved_v));
    }
    bn.destroy();
}
} // namespace yakushima::testing
