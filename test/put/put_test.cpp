/**
 * @file put_put_test.cpp
 */

#include <algorithm>
#include <array>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class put_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

TEST_F(put_test, put_at_non_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST,
              put(token, v, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_at_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(token, st, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_to_root_border_split) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = key_slice_length + 1;
    std::array<std::string, ary_size> k{};
    std::array<std::string, ary_size> v{};
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, 'a' + i);
        v.at(i).assign(1, 'a' + i);
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is interior
    auto* n = ti->load_root_ptr();
    ASSERT_EQ(typeid(*n), typeid(interior_node)); // NOLINT
    auto* in = dynamic_cast<interior_node*>(ti->load_root_ptr());
    // check child is border node which has 8 elements
    auto* bn = dynamic_cast<border_node*>(in->get_child_at(0));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);
    // check that it is not root node
    ASSERT_EQ(bn->get_stable_version().get_root(), false);
    bn = dynamic_cast<border_node*>(in->get_child_at(1));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);
    // check that it is not root node
    ASSERT_EQ(bn->get_stable_version().get_root(), false);

    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(token, st, "k", v.data(), v.size()));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key_twice_unique_rest_true) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    char* created_value_ptr{};
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), true));
    ASSERT_EQ(status::WARN_UNIQUE_RESTRICTION,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), true));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key_twice_unique_rest_false) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    char* created_value_ptr{};
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), false));
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), false));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing