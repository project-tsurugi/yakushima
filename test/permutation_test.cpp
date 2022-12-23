/**
 * @file permutation_test.cpp
 */

#include <thread>

#include "gtest/gtest.h"

#include "permutation.h"

using namespace yakushima;

namespace yakushima::testing {

class pt : public ::testing::Test {};

TEST_F(pt, test1) { // NOLINT
                    // basic member test.
    permutation per{};
    ASSERT_EQ(true, true); // test ending of function which returns void.
    ASSERT_EQ(per.get_cnk(), 0);
    ASSERT_EQ(per.set_cnk(1), status::OK);
    ASSERT_EQ(per.get_cnk(), 1);
    ASSERT_EQ(per.get_cnk(), 1); // check invariant.
}

TEST_F(pt, test2) { // NOLINT
    permutation per{};
    ASSERT_EQ(true, true);
    // per.display();
    per.inc_key_num();
    ASSERT_EQ(true, true);
    // per.display();
}
} // namespace yakushima::testing
