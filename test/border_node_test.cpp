/**
 * @file border_node_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class border_node_test : public ::testing::Test {
protected:
  border_node_test() = default;

  ~border_node_test() = default;
};

TEST_F(border_node_test, display) {
  border_node bn;
  ASSERT_EQ(true, true);
  //bn.display();
  bn.init_border();
  ASSERT_EQ(true, true);
  //bn.display();
}
} // namespace yakushima

