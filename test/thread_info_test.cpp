/**
 * @file thread_info_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"
#include "interior_node.h"
#include "thread_info.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class thread_info_test : public ::testing::Test {
protected:
  thread_info_test() = default;

  ~thread_info_test() = default;
};

TEST_F(thread_info_test, thread_info_init_leave) {
  thread_info::init();
  constexpr std::size_t length = 5;
  Token token[length];
  for (std::size_t i = 0; i < length; ++i) {
    ASSERT_EQ(thread_info::assign_session(token[i]), status::OK);
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
    ASSERT_EQ((thread_info::leave_session<interior_node, border_node>(token[i])), status::OK);
  }
}

}  // namespace yakushima::testing
