/**
 * @file compare_test.cpp
 */

#include <string>

#include "base_node.h"

#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class ct : public ::testing::Test {};

TEST_F(ct, compareTuple) {
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

TEST_F(ct, compareStringView) {
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
  std::string a(1, '\0'), b(2, '\0');
  ASSERT_EQ(a < b, true);
  a.assign(2, 'a');
  b.assign(1, 'b');
  ASSERT_EQ(a < b, true);
  ASSERT_EQ(std::string_view(a) < std::string_view(b), true);
  a.assign(8, 'a');
  std::string_view tmp(a);
  tmp.remove_prefix(8);
  ASSERT_EQ(tmp == std::string_view(0, 0), true);
  a.assign("9");
  b.assign("10");
  ASSERT_EQ(a > b, true);
  ASSERT_EQ(std::string_view(a) > std::string_view(b), true);
#if 0
  /**
   * runtime error
   */
  ASSERT_EQ(string_view(0, 1) == string_view(0, 1), 1);
  std::string a(1, '\0'), b(2, '\0');
  ASSERT_EQ(std::string_view(a), std::string_view(b));
#endif
}
}  // namespace yakushima::testing
