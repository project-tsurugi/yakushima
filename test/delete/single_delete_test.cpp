/**
 * @file delete_test.cpp
 */

#include <algorithm>
#include <array>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class dt : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(dt, delete_at_non_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST, remove(token, "", ""));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, delete_at_existing_storage_but_no_entry) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK_NOT_FOUND, remove(token, test_storage_name, ""));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, // NOLINT
       delete_at_existing_storage_exist_entry_but_not_exist_target_entry) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, "", v.data(), v.size()));
    ASSERT_EQ(status::OK_NOT_FOUND, remove(token, test_storage_name, "a"));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, single_remove) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    /**
   * put one key-value
   */
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string k("a");
    std::string v("v-a");
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k),
                              v.data(), v.size()));
    ASSERT_EQ(status::OK,
              remove(token, test_storage_name, std::string_view(k)));
    ASSERT_NE(ti->load_root_ptr(), nullptr);
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
