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

TEST_F(pt, testinsdel) { // NOLINT
    permutation per{};
    per.insert_rank(0, 0);
    per.insert_rank(0, 1);
    per.insert_rank(0, 2);
    per.insert_rank(3, 3);
    ASSERT_EQ(per.get_cnk(), 4);
    ASSERT_EQ(per.get_index_of_rank(0), 2);
    ASSERT_EQ(per.get_index_of_rank(1), 1);
    ASSERT_EQ(per.get_index_of_rank(2), 0);
    ASSERT_EQ(per.get_index_of_rank(3), 3);
    // per.display();

    per.delete_rank(2);
    EXPECT_EQ(per.get_cnk(), 3);
    EXPECT_EQ(per.get_index_of_rank(0), 2);
    EXPECT_EQ(per.get_index_of_rank(1), 1);
    EXPECT_EQ(per.get_index_of_rank(2), 3);
    per.delete_rank(0);
    EXPECT_EQ(per.get_cnk(), 2);
    EXPECT_EQ(per.get_index_of_rank(0), 1);
    EXPECT_EQ(per.get_index_of_rank(1), 3);
    per.insert_rank(1, 0);
    EXPECT_EQ(per.get_cnk(), 3);
    EXPECT_EQ(per.get_index_of_rank(0), 1);
    EXPECT_EQ(per.get_index_of_rank(1), 0);
    EXPECT_EQ(per.get_index_of_rank(2), 3);
    per.delete_rank(2);
    EXPECT_EQ(per.get_cnk(), 2);
    EXPECT_EQ(per.get_index_of_rank(0), 1);
    EXPECT_EQ(per.get_index_of_rank(1), 0);
    per.delete_rank(0);
    EXPECT_EQ(per.get_cnk(), 1);
    EXPECT_EQ(per.get_index_of_rank(0), 0);
    per.delete_rank(0);
    EXPECT_EQ(per.get_cnk(), 0);
}

} // namespace yakushima::testing
