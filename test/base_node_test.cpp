/**
 * @file base_node_test.cpp
 */

#include "gtest/gtest.h"

#include "base_node.h"
#include "version.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class base_node_test : public ::testing::Test {
protected:
  base_node_test() = default;

  ~base_node_test() = default;
};

TEST_F(base_node_test, constructor_base_node) {
  node_version64 ver;
  ver.init();
  base_node bn;
  ASSERT_EQ(bn.get_version().get_body(), ver.get_body());
  ASSERT_EQ(bn.get_parent(), nullptr);
  for (std::size_t i = 0; i < base_node::node_fanout; ++i) {
    ASSERT_EQ(bn.get_key_slice_at(i), 0);
  }
}

}  // namespace yakushima::testing
