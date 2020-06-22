/**
 * @file operator_test.cpp
 */

#include "gtest/gtest.h"

using std::cout;
using std::endl;
using namespace std;

namespace yakushima::testing {

class operator_test : public ::testing::Test {
protected:
  operator_test() = default;

  ~operator_test() = default;
};

TEST_F(operator_test, exclamation) {
  std::size_t i = 1;
  ASSERT_EQ(!i, 0);
  i = 0;
  ASSERT_EQ(!i, 1);
}
}  // namespace yakushima::testing
