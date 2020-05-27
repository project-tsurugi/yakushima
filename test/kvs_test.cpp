/**
 * @file kvs_test.cpp
 */

#include <future>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

#include "include/global_variables_decralation.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class kvs_test : public ::testing::Test {
protected:
  kvs_test() = default;

  ~kvs_test() = default;
};

TEST_F(kvs_test, init) {
  masstree_kvs::init_kvs();
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  ASSERT_NE(base_node::get_root(), nullptr);
  masstree_kvs::init_kvs();
  ASSERT_EQ(base_node::get_root(), nullptr);
}

TEST_F(kvs_test, put_get) {
  masstree_kvs::init_kvs();
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  ASSERT_NE(base_node::get_root(), nullptr);
  std::tuple<char*, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k));
  ASSERT_NE(std::get<0>(tuple), nullptr);
  ASSERT_EQ(std::get<1>(tuple), v.size());
  ASSERT_EQ(memcmp(&std::get<0>(tuple), v.data(), v.size()), 0);
}

}  // namespace yakushima::testing
