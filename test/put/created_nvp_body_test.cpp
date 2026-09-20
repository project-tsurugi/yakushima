/**
 * @file
 */

#include <iostream>
#include <string>

#include "test_tool.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class created_nvp_body_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(storage_);
    }

    void TearDown() override { fin(); }

    std::string storage_{"1"}; // NOLINT
};

TEST_F(created_nvp_body_test, split_of_root_border) { // NOLINT
    Token token{};
    ASSERT_OK(enter(token));
    std::string v{"v"};
    char* cvp{};
    inserted_node_info ii{};
    for (std::size_t i = 0; i < key_slice_length; i++) {
        ASSERT_OK(put(token, storage_, std::to_string(i), v.data(), v.size(),
                      &cvp, static_cast<value_align_type>(alignof(char)), true,
                      &ii));
    }
    // this put causes a split of the root border node
    ASSERT_OK(put(token, storage_, "b", v.data(), v.size(), &cvp,
                  static_cast<value_align_type>(alignof(char)), true, &ii));
    ASSERT_EQ(ii.created_nvps.size(), 1);
    auto captured = ii.created_nvps[0].first;
    auto actual = ii.created_nvps[0].second->get_body();
    std::cout << "captured: " << captured << std::endl;
    std::cout << "actual  : " << actual << std::endl;
    EXPECT_EQ(captured, actual);
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_OK(leave(token));
}

} // namespace yakushima::testing
