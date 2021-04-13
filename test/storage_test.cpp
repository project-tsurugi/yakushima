/**
 * @file storage_test.cpp
 */

#include "gtest/gtest.h"

#include "storage.h"

using namespace yakushima;

namespace yakushima::testing {

class st : public ::testing::Test {
};

TEST_F(st, prerequisite) {  // NOLINT
    ASSERT_EQ(std::is_trivially_copyable<std::atomic<base_node*>>::value, true);
    std::atomic<base_node*> aptr1{(base_node*)((void*)std::uintptr_t(1))};
    std::atomic<base_node*> aptr2{(base_node*)((void*)std::uintptr_t(2))};
    std::memcpy(&aptr1, &aptr2, sizeof(std::atomic<base_node*>));
    ASSERT_EQ((void*)aptr1.load(std::memory_order_acquire), (void*)std::uintptr_t(2));
}

TEST_F(st, basic_test) { // NOLINT
    ASSERT_EQ(true, true);
}

}  // namespace yakushima::testing
