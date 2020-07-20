/**
 * @file kvs_test.cpp
 */

#include <array>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class kvs_test : public ::testing::Test {
protected:
  void SetUp() override {
    masstree_kvs::init();
  }

  void TearDown() override {
    masstree_kvs::fin();
  }
};

TEST_F(kvs_test, destroy) { // NOLINT
  Token token{};
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::array<std::string, 50> k{}; // NOLINT
  std::string v("value");
  std::size_t ctr{1};
  for (auto&& itr : k) {
    itr = std::string(1, ctr); // NOLINT
    ++ctr;
    ASSERT_EQ(status::OK, masstree_kvs::put(itr, v.data(), v.size()));
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
  // destroy test by using destructor (fin());
}

TEST_F(kvs_test, init) { // NOLINT
  ASSERT_EQ(base_node::get_root(), nullptr);
  Token token{};
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::string k("a");
  std::string v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(k, v.data(), v.size()));
  ASSERT_NE(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

}  // namespace yakushima::testing
