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

TEST_F(border_node_test, shift_left_border_member) {
  border_node bn;
  link_or_value lv;
  for (std::size_t i = 0; i < 15; ++i) {
    lv.set_value_length(i);
    bn.set_lv(i, &lv);
  }
  for (std::size_t i = 0; i < 5; ++i) {
    link_or_value lv2 = *bn.get_lv_at(0);
    bn.shift_left_border_member(1, 1);
    ASSERT_EQ(lv2.get_value_length() + 1, bn.get_lv_at(0)->get_value_length());
  }
}
} // namespace yakushima

