/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class st : public ::testing::Test {
    void SetUp() override {
        init();
    }

    void TearDown() override {
        fin();
    }
};

TEST_F(st, basic_usage) { // NOLINT
    /**
     * basic usage
     */
    ASSERT_EQ(base_node::get_root_ptr(), nullptr);
    std::string k("k");
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(std::string_view(k), v.data(), v.size()));
    std::vector<std::pair<char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) return false;
        if (tup_lis.size() != nv.size()) return false;
        if (std::get<1>(tup_lis.at(0)) != v.size()) return false;
        if (memcmp(std::get<0>(tup_lis.at(0)), v.data(), v.size()) != 0) return false;
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) return false;
        if (tup_lis.size() != nv.size()) return false;
        return true;
    };
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE, scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE, scan<char>("", scan_endpoint::INF, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima

