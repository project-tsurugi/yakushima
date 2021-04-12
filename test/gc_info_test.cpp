/**
 * @file thread_info_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"
#include "gc_info.h"
#include "gc_info_table.h"

using namespace yakushima;

namespace yakushima::testing {

class tit : public ::testing::Test {
};

TEST_F(tit, test1) { // NOLINT
    gc_info_table::init();
    constexpr std::size_t length = 5;
    std::array<Token, length> token; // NOLINT
    for (auto &&elem : token) {
        ASSERT_EQ(gc_info_table::assign_gc_info(elem), status::OK);
    }
    for (auto &&elem : token) {
        gc_info* ti = reinterpret_cast<gc_info*>(elem); // NOLINT
        ASSERT_EQ(ti->get_running(), true);
        ASSERT_NE(ti->get_begin_epoch(), 0);
        ASSERT_NE(ti->get_node_container(), nullptr);
        ASSERT_NE(ti->get_value_container(), nullptr);
        for (auto &&elem2 : token) {
            if (elem == elem2) continue;
            ASSERT_NE(elem, elem2);
        }
    }

    for (auto &&elem : token) {
        ASSERT_EQ((gc_info_table::leave_gc_info<interior_node, border_node>(elem)), status::OK);
    }
}
}  // namespace yakushima::testing
