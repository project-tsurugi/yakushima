/**
 * @file thread_info_test.cpp
 */

#include "gtest/gtest.h"

#include "border_node.h"
#include "thread_info.h"
#include "thread_info_table.h"

using namespace yakushima;

namespace yakushima::testing {

class tit : public ::testing::Test {};

TEST_F(tit, test1) { // NOLINT
    thread_info_table::init();
    constexpr std::size_t length = 5;
    std::array<Token, length> token; // NOLINT
    for (auto&& elem : token) {
        ASSERT_EQ(thread_info_table::assign_thread_info(elem), status::OK);
    }
    for (auto&& elem : token) {
        auto* ti = reinterpret_cast<thread_info*>(elem); // NOLINT
        ASSERT_EQ(ti->get_running(), true);
        ASSERT_NE(ti->get_begin_epoch(), 0);
        for (auto&& elem2 : token) {
            if (elem == elem2) { continue; }
            ASSERT_NE(elem, elem2);
        }
    }

    for (auto&& elem : token) {
        ASSERT_EQ((thread_info_table::leave_thread_info<interior_node,
                                                        border_node>(elem)),
                  status::OK);
    }
}
} // namespace yakushima::testing
