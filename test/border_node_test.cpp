/**
 * @file border_node_test.cpp
 */

#include <mutex>

#include "gtest/gtest.h"

#include "border_node.h"

using namespace yakushima;

namespace yakushima::testing {

class border_node_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-border_node_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { std::call_once(init_, call_once_f); }

    void TearDown() override {}

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(border_node_test, alignment) { // NOLINT
    ASSERT_EQ(alignof(border_node), CACHE_LINE_SIZE);
}

TEST_F(border_node_test, display) { // NOLINT
    border_node bn;
    ASSERT_EQ(true, true);
    // bn.display();
    bn.init_border();
    ASSERT_EQ(true, true);
    // bn.display();
}

} // namespace yakushima::testing
