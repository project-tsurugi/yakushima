/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"s"}; // NOLINT

class scan_reverse_test : public ::testing::Test {
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

std::string_view key(std::tuple<std::string, char*, std::size_t> const& t) {
    return std::get<0>(t);
}

std::string_view value(std::tuple<std::string, char*, std::size_t> const& t) {
    return std::string_view{std::get<1>(t), std::get<2>(t)};
}

TEST_F(scan_reverse_test, basic_usage) { // NOLINT
    std::string k0("k0");
    std::string k1("k1");
    std::string v0("v0");
    std::string v1("v1");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, st, k0, v0.data(), v0.size()));
    ASSERT_EQ(status::OK, put(token, st, k1, v1.data(), v1.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v1]() {
        if (tup_lis.size() != 1) { return false; }
        if (tup_lis.size() != nv.size()) { return false; }
        if (std::get<2>(tup_lis.at(0)) != v1.size()) { return false; }
        if (memcmp(std::get<1>(tup_lis.at(0)), v1.data(), v1.size()) != 0) {
            return false;
        }
        return true;
    };
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv, 1, true));

    ASSERT_EQ(true, verify());
    ASSERT_EQ(tup_lis.size(), 1);
    EXPECT_EQ(key(tup_lis[0]), k1);
    EXPECT_EQ(value(tup_lis[0]), v1);

    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv, 1, true));
    ASSERT_EQ(tup_lis.size(), 1);
    EXPECT_EQ(key(tup_lis[0]), k1);
    EXPECT_EQ(value(tup_lis[0]), v1);

    // currently max_size must be 1 and r_end == INF for reverse scan
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(st, "", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv, 1, true));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(st, "", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv, 0, true));

    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(scan_reverse_test, scan_results_zero) { // NOLINT
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK,
              scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv, 1, true));
    ASSERT_EQ(status::OK, leave(s));
}

TEST_F(scan_reverse_test, long_key_scan) { // NOLINT
    // prepare
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::string st{"test"};
    ASSERT_EQ(status::OK, create_storage(st));

    for (std::size_t i = 1024; i <= 1024 * 30; i += 1024) { // NOLINT
        // put
        LOG(INFO) << "test key size " << i / 1024 << " KiB";
        std::string k0(i, 'a');
        std::string k1(i, 'b');
        std::string v0{"v0"};
        std::string v1{"v1"};
        ASSERT_EQ(status::OK, put(s, st, k0, v0.data(), v0.size()));
        ASSERT_EQ(status::OK, put(s, st, k1, v1.data(), v1.size()));

        // test: scan
        std::vector<std::tuple<std::string, char*, std::size_t>> tup_lis{}; // NOLINT
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv, 1, true));
        ASSERT_EQ(tup_lis.size(), 1);
        EXPECT_EQ(key(tup_lis[0]), k1);
        EXPECT_EQ(value(tup_lis[0]), v1);
    }

    // cleanup
    ASSERT_EQ(status::OK, leave(s));
}

TEST_F(scan_reverse_test, scan_single_border) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    for (char i = 0; i <= 7; ++i) { // NOLINT
        char c = i;
        std::string v{"v"};
        v += std::to_string(i);
        ASSERT_EQ(status::OK, put(token, st, std::string_view(&c, 1), v.data(), v.size()));
    }
    /**
     * border node
     * 0,  1,  2,  3,  4,  5,  6,  7
     */
    std::vector<std::tuple<std::string, char*, std::size_t>> tup{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup, &nv, 1, true));
    ASSERT_EQ(tup.size(), 1); // NOLINT
    ASSERT_EQ(tup.size(), nv.size()); // NOLINT
    EXPECT_EQ(key(tup[0]), "\x07");
    EXPECT_EQ(value(tup[0]), "v7");

    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(scan_reverse_test, scan_two_borders) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    for (char i = 0; i <= 16; ++i) { // NOLINT
        char c = i;
        std::string v{"v"};
        v += std::to_string(i);
        ASSERT_EQ(status::OK, put(token, st, std::string_view(&c, 1), v.data(), v.size()));
    }
    /**
     * border nodes
     * A:  0,  1,  2,  3,  4,  5,  6,  7,
     * B:  8,  9, 10, 11, 12, 13, 14, 15, 16
     */
    std::vector<std::tuple<std::string, char*, std::size_t>> tup{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup, &nv, 1, true));
    ASSERT_EQ(tup.size(), 1); // NOLINT
    ASSERT_EQ(tup.size(), nv.size()); // NOLINT

    EXPECT_EQ(key(tup[0]), "\x10");
    EXPECT_EQ(value(tup[0]), "v16");
}

TEST_F(scan_reverse_test, scan_three_borders) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    for (char i = 0; i <= 25; ++i) { // NOLINT
        char c = i;
        std::string v{"v"};
        v += std::to_string(i);
        ASSERT_EQ(status::OK,
                  put(token, st, std::string_view(&c, 1), v.data(), v.size()));
    }
    /**
     * now
     * A:  0,  1,  2,  3,  4,  5,  6,  7,
     * B:  8,  9, 10, 11, 12, 13, 14, 15,
     * C: 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
     * branch of A and B is 8
     * branch of B and C is 16
     */
    std::vector<std::tuple<std::string, char*, std::size_t>> tup{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup, &nv, 1, true));
    ASSERT_EQ(tup.size(), 1); // NOLINT
    ASSERT_EQ(tup.size(), nv.size()); // NOLINT
    EXPECT_EQ(key(tup[0]), "\x19");
    EXPECT_EQ(value(tup[0]), "v25");
}

TEST_F(scan_reverse_test, scan_three_borders_removed_last) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    for (char i = 0; i <= 25; ++i) { // NOLINT
        char c = i;
        std::string v{"v"};
        v += std::to_string(i);
        ASSERT_EQ(status::OK,
                  put(token, st, std::string_view(&c, 1), v.data(), v.size()));
    }
    /**
     * now
     * A:  0,  1,  2,  3,  4,  5,  6,  7,
     * B:  8,  9, 10, 11, 12, 13, 14, 15,
     * C: 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
     * branch of A and B is 8
     * branch of B and C is 16
     */
    std::vector<std::tuple<std::string, char*, std::size_t>> tup{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup, &nv, 1, true));
    ASSERT_EQ(tup.size(), 1); // NOLINT
    ASSERT_EQ(tup.size(), nv.size()); // NOLINT

    auto delete_range = [&token](char begin, char end) {
        for (char i = begin; i <= end; ++i) {
            char c = i;
            ASSERT_EQ(status::OK, remove(token, st, std::string_view(&c, 1)));
        }
    };
    delete_range(25, 25); // NOLINT
    /**
     * now
     * A:  0,  1,  2,  3,  4,  5,  6,  7,
     * B:  8,  9, 10, 11, 12, 13, 14, 15,
     * C: 16, 17, 18, 19, 20, 21, 22, 23, 24
     */
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF, tup, &nv, 1, true));
    ASSERT_EQ(tup.size(), 1); // NOLINT
    EXPECT_EQ(key(tup[0]), "\x18");
    EXPECT_EQ(value(tup[0]), "v24");

    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
