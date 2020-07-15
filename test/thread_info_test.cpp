/**
 * @file thread_info_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"
#include "thread_info.h"
#include "thread_info_table.h"

using namespace yakushima;

namespace yakushima::testing {

class tit : public ::testing::Test {
protected:
  tit() = default;

  ~tit() = default;
};

TEST_F(tit, test1) { // NOLINT
  thread_info_table::init();
  constexpr std::size_t length = 5;
  Token token[length];
  for (std::size_t i = 0; i < length; ++i) {
    ASSERT_EQ(thread_info_table::assign_session(token[i]), status::OK);
  }
  for (std::size_t i = 0; i < length; ++i) {
    thread_info *ti = reinterpret_cast<thread_info *>(token[i]);
    ASSERT_EQ(ti->get_running(), true);
    ASSERT_EQ(ti->get_begin_epoch(), 0);
    ASSERT_NE(ti->get_node_container(), nullptr);
    ASSERT_NE(ti->get_value_container(), nullptr);
    for (std::size_t j = 0; j < length; ++j) {
      if (i == j) continue;
      ASSERT_NE(token[i], token[j]);
    }
  }

  for (std::size_t i = 0; i < length; ++i) {
    ASSERT_EQ((thread_info_table::leave_session<interior_node, border_node>(token[i])), status::OK);
  }
}

}  // namespace yakushima::testing
