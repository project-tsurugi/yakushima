/**
 * @file base_node_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"
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
  /**
   * base_node class is abstract class, so it can not declare object as base_node type.
   * Instead of that, it uses border_node.
   */
  border_node bn;
  ASSERT_EQ(bn.get_version(), ver.get_body());
  ASSERT_EQ(bn.get_parent(), nullptr);
  for (std::size_t i = 0; i < base_node::key_slice_length; ++i) {
    ASSERT_EQ(bn.get_key_slice_at(i), 0);
  }
}

TEST_F(base_node_test, type_size) {
  /**
   * kvs.h uses the argument (sizeof(base_node::key_slice_type)) as 8 at std::string_view::remove_suffix function.
   */
  ASSERT_EQ(sizeof(base_node::key_slice_type), 8);
}

}  // namespace yakushima::testing
