/**
 * @file base_node_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"

using namespace yakushima;

namespace yakushima::testing {

class bnt : public ::testing::Test {};

TEST_F(bnt, constructor) { // NOLINT
    node_version64 ver;
    ver.init();
    /**
     * base_node class is abstract class, so it can not declare object as base_node type.
     * Instead of that, it uses border_node.
     */
    border_node bn;
    ASSERT_EQ(bn.get_version(), ver.get_body());
    ASSERT_EQ(bn.get_parent(), nullptr);
    for (std::size_t i = 0; i < key_slice_length; ++i) {
        ASSERT_EQ(bn.get_key_slice_at(i), 0);
    }
}

TEST_F(bnt, typeSize) { // NOLINT
    /**
     * kvs.h uses the argument (sizeof(key_slice_type)) as 8 at
     * std::string_view::remove_suffix function.
     */
    ASSERT_EQ(sizeof(key_slice_type), 8);
}

TEST_F(bnt, function) { // NOLINT
    border_node bn;
    struct S {
        static void init(border_node& bn) {
            for (std::size_t i = 0; i < key_slice_length; ++i) {
                bn.set_key(i, i, static_cast<key_length_type>(i));
            }
        }
    };
    S::init(bn);
    bn.shift_right_base_member(0, 1); // left-most
    for (std::size_t i = 1; i < key_slice_length; ++i) {
        ASSERT_EQ(bn.get_key_slice_at(i), i - 1);
    }
    S::init(bn);
    bn.shift_right_base_member(5, 1); // middle-point
    for (std::size_t i = 1; i < key_slice_length; ++i) {
        if (i < 6) {
            ASSERT_EQ(bn.get_key_slice_at(i), i);
        } else {
            ASSERT_EQ(bn.get_key_slice_at(i), i - 1);
        }
    }
}

TEST_F(bnt, key_tuple) { // NOLINT
    // regular case
    char d1[8] = { 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // NOLINT
    char d2[8] = { 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }; // NOLINT
    std::uint64_t ud1 = *reinterpret_cast<std::uint64_t*>(d1); // NOLINT
    std::uint64_t ud2 = *reinterpret_cast<std::uint64_t*>(d2); // NOLINT
    ASSERT_LT(base_node::key_tuple(ud1, 1), base_node::key_tuple(ud2, 1));
    ASSERT_LT(base_node::key_tuple(ud1, 1), base_node::key_tuple(ud2, 2));
    ASSERT_LT(base_node::key_tuple(ud1, 2), base_node::key_tuple(ud2, 1));
    ASSERT_LT(base_node::key_tuple(ud1, 1).compare(base_node::key_tuple(ud2, 1)), 0);
    ASSERT_LT(base_node::key_tuple(ud1, 1).compare(base_node::key_tuple(ud2, 2)), 0);
    ASSERT_LT(base_node::key_tuple(ud1, 2).compare(base_node::key_tuple(ud2, 1)), 0);
    // suffix NUL
    ASSERT_LT(base_node::key_tuple(ud1, 1), base_node::key_tuple(ud1, 2));
    ASSERT_LT(base_node::key_tuple(ud1, 2), base_node::key_tuple(ud1, 3));
    ASSERT_LT(base_node::key_tuple(ud1, 3), base_node::key_tuple(ud1, 4));
    ASSERT_LT(base_node::key_tuple(ud1, 1).compare(base_node::key_tuple(ud1, 2)), 0);
    ASSERT_LT(base_node::key_tuple(ud1, 2).compare(base_node::key_tuple(ud1, 3)), 0);
    ASSERT_LT(base_node::key_tuple(ud1, 3).compare(base_node::key_tuple(ud1, 4)), 0);

    // empty case
    ASSERT_EQ(base_node::key_tuple{}, base_node::key_tuple{});
    ASSERT_FALSE(base_node::key_tuple{} < base_node::key_tuple{});
    ASSERT_EQ(base_node::key_tuple{}.compare(base_node::key_tuple{}), 0);
}

} // namespace yakushima::testing
