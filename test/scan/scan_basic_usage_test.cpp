/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

#include "test_tool.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class st : public ::testing::Test {
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(st, scan_at_non_existing_stoarge) { // NOLINT
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

TEST_F(st, basic_usage) { // NOLINT
    /**
     * basic usage
     */
    std::string k("k");
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k),
                              v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) { return false; }
        if (tup_lis.size() != nv.size()) { return false; }
        if (std::get<2>(tup_lis.at(0)) != v.size()) { return false; }
        if (memcmp(std::get<1>(tup_lis.at(0)), v.data(), v.size()) != 0) {
            return false;
        }
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) { return false; }
        if (nv.size() != 1) { return false; }
        return true;
    };
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF,
                                     "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, inf_endpoint) {
    // choose endpoint=inf case of basic, and change key not to empty
    // key must be ignored, so same result
    std::string k("k");
    std::string v("v");
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put(token, test_storage_name, k, v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{};
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) { return false; }
        if (tup_lis.size() != nv.size()) { return false; }
        if (std::get<2>(tup_lis.at(0)) != v.size()) { return false; }
        if (memcmp(std::get<1>(tup_lis.at(0)), v.data(), v.size()) != 0) {
            return false;
        }
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) { return false; }
        if (nv.size() != 1) { return false; }
        return true;
    };
    // a < k < z
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF,
                         "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "a",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "a",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_OK(leave(token));
}

TEST_F(st, inf_endpoint_l1) {
    // long key (Layer 1+) version of inf_endpoint
    std::string k("kkkkkkkkk");
    std::string v("v");
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put(token, test_storage_name, k, v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{};
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) { return false; }
        //if (tup_lis.size() != nv.size()) { return false; }
        if (std::get<2>(tup_lis.at(0)) != v.size()) { return false; }
        if (memcmp(std::get<1>(tup_lis.at(0)), v.data(), v.size()) != 0) {
            return false;
        }
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) { return false; }
        //if (nv.size() != 1) { return false; }
        return true;
    };
    // a < kkkkkkkkk < z
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF,
                         "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "a",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "a",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_OK(leave(token));
}

TEST_F(st, inf_endpoint_border_chain) {
    // find_border
    std::string v("v");
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put(token, test_storage_name, "000000000", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "111111111", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "222222222", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "333333333", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "444444444", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "555555555", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "666666666", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "777777777", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "888888888", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "999999999", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "aaaaaaaaa", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "bbbbbbbbb", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "ccccccccc", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "ddddddddd", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "eeeeeeeee", v.data(), v.size()));
    ASSERT_OK(put(token, test_storage_name, "fffffffff", v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{};
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    // 0 < keys... < z
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF,
                         "", scan_endpoint::INF, tup_lis, &nv));
    EXPECT_EQ(tup_lis.size(), 16);
    ASSERT_OK(scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    EXPECT_EQ(tup_lis.size(), 0);
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "0",
                         scan_endpoint::INF, tup_lis, &nv));
    EXPECT_EQ(tup_lis.size(), 16);
    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::EXCLUSIVE, "0",
                         scan_endpoint::INF, tup_lis, &nv));
    EXPECT_EQ(tup_lis.size(), 16);
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, "z", scan_endpoint::INF, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_OK(leave(token));
}

} // namespace yakushima::testing
