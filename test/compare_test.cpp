/**
 * @file compare_test.cpp
 */

#include <array>

#include "base_node.h"

#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class ct : public ::testing::Test {
};

TEST_F(ct, compareBool) { // NOLINT
  ASSERT_EQ(false < true, true);
}

TEST_F(ct, compareData) { // NOLINT
  std::string s1("aac");                        // NOLINT
  std::string s2("b");                          // NOLINT
  std::uint64_t k1{};
  std::uint64_t k2{};
  memcpy(&k1, s1.data(), s1.size());
  memcpy(&k2, s2.data(), s2.size());
  ASSERT_NE(s1 < s2, k1 < k2);
  ASSERT_EQ(s1 < s2, memcmp(&k1, &k2, sizeof(std::uint64_t)) < 0);
  ASSERT_EQ(s1 < s2, std::string_view(reinterpret_cast<char *>(&k1), sizeof(std::uint64_t)) < // NOLINT
                     std::string_view(reinterpret_cast<char *>(&k2), sizeof(std::uint64_t))); // NOLINT
}

TEST_F(ct, compareTuple) { // NOLINT
  std::array<base_node::key_slice_type, 2> key_slice{};
  std::array<base_node::key_length_type, 2> key_length{};
  key_slice.at(0) = 0;
  key_length.at(0) = 0;
  key_slice.at(1) = 0;
  key_length.at(1) = 1;
  std::array<std::tuple<base_node::key_slice_type, base_node::key_length_type>, 2> tuple; // NOLINT
  tuple.at(0) = std::make_tuple(key_slice.at(0), key_length.at(0));
  tuple.at(1) = std::make_tuple(key_slice.at(1), key_length.at(1));
  ASSERT_EQ(tuple.at(0) < tuple.at(1), true);
}

TEST_F(ct, compareStringView) { // NOLINT
  std::array<base_node::key_slice_type, 2> key_slice{};
  std::array<base_node::key_length_type, 2> key_length{};
  key_slice.at(0) = 0;
  key_length.at(0) = 0;
  key_slice.at(1) = 0;
  key_length.at(1) = 1;
  ASSERT_EQ((std::string_view(reinterpret_cast<char *>(&key_slice.at(0)), key_length.at(0)) < // NOLINT
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  ASSERT_NE((std::string_view(reinterpret_cast<char *>(&key_slice.at(0)), key_length.at(0)) == // NOLINT
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  ASSERT_NE((std::string_view(reinterpret_cast<char *>(&key_slice.at(0)), key_length.at(0)) > // NOLINT
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  ASSERT_EQ((std::string_view(nullptr, 0) <
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  ASSERT_NE((std::string_view(nullptr, 0) ==
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  ASSERT_NE((std::string_view(nullptr, 0) >
             std::string_view{reinterpret_cast<char *>(&key_slice.at(1)), key_length.at(1)}), true); // NOLINT
  std::string a(1, '\0');
  std::string b(2, '\0');
  ASSERT_EQ(a < b, true);
  a.assign(2, 'a');
  b.assign(1, 'b');
  ASSERT_EQ(a < b, true);
  ASSERT_EQ(std::string_view(a) < std::string_view(b), true);
  a.assign(8, 'a');
  std::string_view tmp(a);
  tmp.remove_prefix(8);
  ASSERT_EQ(tmp == std::string_view(nullptr, 0), true);
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
