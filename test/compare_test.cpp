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
using namespace std;

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

TEST_F(compare_test, compare_string_view) {
  base_node::key_slice_type key_slice[2];
  base_node::key_length_type key_length[2];
  key_slice[0] = 0;
  key_length[0] = 0;
  key_slice[1] = 0;
  key_length[1] = 1;
  ASSERT_EQ((std::string_view(reinterpret_cast<char *>(&key_slice[0]), key_length[0]) <
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  ASSERT_NE((std::string_view(reinterpret_cast<char *>(&key_slice[0]), key_length[0]) ==
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  ASSERT_NE((std::string_view(reinterpret_cast<char *>(&key_slice[0]), key_length[0]) >
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  ASSERT_EQ((std::string_view(0, 0) <
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  ASSERT_NE((std::string_view(0, 0) ==
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  ASSERT_NE((std::string_view(0, 0) >
             std::string_view{reinterpret_cast<char *>(&key_slice[1]), key_length[1]}), true);
  // runtime error
  // ASSERT_EQ(string_view(0, 1) == string_view(0, 1), 1);
}
}  // namespace yakushima::testing
