/**
 * @file link_or_value_test.cpp
 */

#include <future>

#include "gtest/gtest.h"

#include "link_or_value.h"

using namespace yakushima;

namespace yakushima::testing {

class lvtest : public ::testing::Test {};

TEST_F(lvtest, typeAssert) { // NOLINT
    ASSERT_EQ(std::is_trivially_copyable<link_or_value>::value, true);
}

TEST_F(lvtest, display) { // NOLINT
    link_or_value lv;
    ASSERT_EQ(true, true);
    // lv.display();
    lv.init_lv();
    ASSERT_EQ(true, true);
    // lv.display();
}

} // namespace yakushima::testing
