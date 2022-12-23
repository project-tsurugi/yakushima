/**
 * @file operator_test.cpp
 */

#include "gtest/gtest.h"

namespace yakushima::testing {

class ot : public ::testing::Test { // NOLINT
};

TEST_F(ot, exclamation) { // NOLINT
    std::size_t i = 1;
    ASSERT_EQ(!i, 0);
    i = 0;
    ASSERT_EQ(!i, 1);
}
} // namespace yakushima::testing
