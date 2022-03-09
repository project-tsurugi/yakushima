/**
 * @file put_get_test.cpp
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

class get_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

TEST_F(get_test, simple) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string dummy_st{"dm"};
    std::pair<char*, std::size_t> out{};
    ASSERT_EQ(status::WARN_NOT_EXIST, get<char>(st, "", out));
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST, get<char>(dummy_st, "", out));
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
