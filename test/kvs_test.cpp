/**
 * @file kvs_test.cpp
 */

#include <future>
#include <thread>

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
  ASSERT_EQ(true, true); // test ending of function which returns void.
}

}  // namespace yakushima::testing
