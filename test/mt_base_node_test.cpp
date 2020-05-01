/**
 * @file mt_base_node_test.cpp
 */

#include "gtest/gtest.h"

#include "mt_base_node.h"
#include "mt_version.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class mt_base_node_test : public ::testing::Test {
protected:
  mt_base_node_test() = default;
  ~mt_base_node_test() = default;
};

TEST_F(mt_base_node_test, constructor_base_node) {
  node_version64 ver;
  ver.init();
  base_node bn;
  ASSERT_EQ(bn.get_version().get_body(), ver.get_body());
  ASSERT_EQ(bn.get_parent(), nullptr);
  for (std::size_t i = 0; i < base_node::key_slice_length; ++i) {
    ASSERT_EQ(bn.get_key_slice_at(i), 0);
  }
}

}  // namespace yakushima::testing
