/**
 * @file interior_node_test.cpp
 */

#include "gtest/gtest.h"

#include "interior_node.h"

using namespace yakushima;

namespace yakushima::testing {

class intest : public ::testing::Test {
};

TEST_F(intest, alignment) { // NOLINT
    ASSERT_EQ(alignof(interior_node), CACHE_LINE_SIZE);
}

TEST_F(intest, display) { // NOLINT
    interior_node in;
    ASSERT_EQ(true, true);
    //in.display();
    in.init_interior();
    ASSERT_EQ(true, true);
    //in.display();
}
} // namespace yakushima::testing
