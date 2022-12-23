/**
 * @file clock_test.cpp
 */

#include "gtest/gtest.h"

#include "clock.h"

using namespace yakushima;

namespace yakushima::testing {

class ct : public ::testing::Test {};

TEST_F(ct, checkClockSpan) { // NOLINT
    std::uint64_t start{10};
    std::uint64_t stop{20};
    ASSERT_EQ(check_clock_span(start, stop, 5), true);
    ASSERT_EQ(check_clock_span(start, stop, 15), false);
    // occur std::abort
    // ASSERT_EQ(check_clock_span(stop, start, 15), false);
}

} // namespace yakushima::testing
