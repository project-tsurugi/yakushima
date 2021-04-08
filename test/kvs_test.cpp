/**
 * @file kvs_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class kvs_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override {
        fin();
    }
};

TEST_F(kvs_test, destroy) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::array<std::string, 50> k{}; // NOLINT
    std::string v("value");
    std::size_t ctr{1};
    for (auto &&itr : k) {
        itr = std::string(1, ctr); // NOLINT
        ++ctr;
        ASSERT_EQ(status::OK, put(test_storage_name, itr, v.data(), v.size()));
    }
    ASSERT_EQ(leave(token), status::OK);
    // destroy test by using destructor (fin());
}

TEST_F(kvs_test, init) { // NOLINT
    std::atomic<base_node*>* target_storage{};
    find_storage(test_storage_name, &target_storage);
    ASSERT_EQ(target_storage->load(std::memory_order_acquire), nullptr);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string k("a");
    std::string v("v-a");
    ASSERT_EQ(status::OK, put(test_storage_name, k, v.data(), v.size()));
    ASSERT_NE(target_storage->load(std::memory_order_acquire), nullptr);
    ASSERT_EQ(leave(token), status::OK);
}

}  // namespace yakushima::testing
