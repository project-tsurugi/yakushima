/**
 * @file compare_test.cpp
 */

#include <string>
#include <string_view>

#include "base_node.h"

#include "gtest/gtest.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class compare_test : public ::testing::Test {
protected:
  compare_test() = default;

  ~compare_test() = default;
};

TEST_F(compare_test, compare_tuple) {
  base_node::key_slice_type key_slice[2];
  base_node::key_length_type key_length[2];
  key_slice[0] = 0;
  key_length[0] = 0;
  key_slice[1] = 0;
  key_length[1] = 1;
  std::tuple<base_node::key_slice_type, base_node::key_length_type> tuple[2];
  tuple[0] = std::make_tuple(key_slice[0], key_length[0]);
  tuple[1] = std::make_tuple(key_slice[1], key_length[1]);
  ASSERT_EQ(tuple[0] < tuple[1], true);
}

}  // namespace yakushima::testing
