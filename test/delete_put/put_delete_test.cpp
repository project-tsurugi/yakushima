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

class put_delete : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(put_delete, simple) { // NOLINT
    Token token{};
    std::string k("k");
    std::string v("v");
    for (std::size_t i = 0; i < 100; ++i) { // NOLINT
        ASSERT_EQ(status::OK, enter(token));
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, k, v.data(), v.size()));
        ASSERT_EQ(status::OK, leave(token));
        ASSERT_EQ(status::OK, enter(token));
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k));
        ASSERT_EQ(status::OK, leave(token));
    }
}

} // namespace yakushima::testing
