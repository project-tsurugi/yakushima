/**
 * @file typeid_test.cpp
 */

#include <memory>

#include "gtest/gtest.h"

#include "base_node.h"
#include "border_node.h"

using namespace yakushima;

namespace yakushima::testing {

class tt : public ::testing::Test {};

TEST_F(tt, test1) { // NOLINT
    std::unique_ptr<border_node> border_uptr =
            std::make_unique<border_node>(); // NOLINT
    std::unique_ptr<interior_node> interior_uptr =
            std::make_unique<interior_node>(); // NOLINT

    ASSERT_EQ(typeid(border_uptr.get()), typeid(border_node*));
    ASSERT_EQ(typeid(interior_uptr.get()), typeid(interior_node*));

    base_node* base_nptr{};                                      // n ... normal
    base_nptr = reinterpret_cast<base_node*>(border_uptr.get()); // NOLINT
    ASSERT_EQ(typeid(*base_nptr), typeid(border_node));
    base_nptr = reinterpret_cast<base_node*>(interior_uptr.get()); // NOLINT
    ASSERT_EQ(typeid(*base_nptr), typeid(interior_node));

    // test real object
    border_node border{};
    interior_node interior{};
    base_nptr = reinterpret_cast<base_node*>(&border); // NOLINT
    ASSERT_EQ(typeid(*base_nptr), typeid(border_node));
    base_nptr = reinterpret_cast<base_node*>(&interior); // NOLINT
    ASSERT_EQ(typeid(*base_nptr), typeid(interior_node));
}

} // namespace yakushima::testing
