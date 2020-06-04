/**
 * @file clock_test.cpp
 */

#include "gtest/gtest.h"

#include "clock.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class clock_test : public ::testing::Test {
protected:
  clock_test() = default;

  ~clock_test() = default;
};

TEST_F(clock_test, check_clock_span) {
  std::uint64_t start, stop;
  start = 10;
  stop = 20;
  ASSERT_EQ(check_clock_span(start, stop, 5), true);
  ASSERT_EQ(check_clock_span(start, stop, 15), false);
  // occur std::abort
  // ASSERT_EQ(check_clock_span(stop, start, 15), false);
}

}  // namespace yakushima::testing
