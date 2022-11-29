/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class scan_test : public ::testing::Test {
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

TEST_F(scan_test, scan_results_zero) { // NOLINT
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INCLUSIVE, "",
                                     scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::OK, leave(s));
}

TEST_F(scan_test, scan_at_non_existing_stoarge) { // NOLINT
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST,
              scan<char>("", "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::OK, leave(s));
}

} // namespace yakushima::testing
