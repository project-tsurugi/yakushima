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
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST, put(token, v, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_at_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(token, st, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
