/**
 * @file interior_node_test.cpp
 */

#include "gtest/gtest.h"

#include "interior_node.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class interior_node_test : public ::testing::Test {
protected:
  interior_node_test() = default;

  ~interior_node_test() = default;
};

TEST_F(interior_node_test, display) {
  interior_node in;
  in.display();
  in.init_interior();
  in.display();
}
}  // namespace yakushima::testing
