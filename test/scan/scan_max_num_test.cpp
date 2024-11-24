/**
 * @file scan_max_num_test.cpp
 */

#include "gtest/gtest.h"

#include "kvs.h"

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

TEST_F(st, one_border) { // NOLINT
    std::string k1("1");
    std::string k2("2");
    std::string k3("3");
    std::string v("v");
    Token token{};
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tuple_list{}; // NOLINT
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k1),
                              v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k2),
                              v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k3),
                              v.data(), v.size()));
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr));
    ASSERT_EQ(tuple_list.size(), 3);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 0));
    ASSERT_EQ(tuple_list.size(), 3);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 1));
    ASSERT_EQ(tuple_list.size(), 1);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 2));
    ASSERT_EQ(tuple_list.size(), 2);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 3));
    ASSERT_EQ(tuple_list.size(), 3);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, reach_limit_on_border) { // NOLINT
    //                    aaaaaaaabbbbbbbb
    std::string_view k11("0000000111111111");
    std::string_view k12("0000000111111112");
    std::string_view k13("0000000111111113");
    std::string_view k21("0000000222222221");
    std::string_view k22("0000000222222222");
    std::string_view k23("0000000222222223");
    std::string_view k31("0000000333333331");
    std::string_view k32("0000000333333332");
    std::string_view k33("0000000333333333");
    std::string v("v");
    Token token{};
    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, k11, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k12, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k13, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k21, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k22, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k23, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k31, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k32, v.data(), v.size()));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k33, v.data(), v.size()));
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr));
    ASSERT_EQ(tuple_list.size(), 9);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 0));
    ASSERT_EQ(tuple_list.size(), 9);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 1));
    ASSERT_EQ(tuple_list.size(), 1);
    EXPECT_EQ(std::get<0>(tuple_list.at(0)), k11);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 2));
    ASSERT_EQ(tuple_list.size(), 2);
    EXPECT_EQ(std::get<0>(tuple_list.at(0)), k11);
    EXPECT_EQ(std::get<0>(tuple_list.at(1)), k12);
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INF, tuple_list, nullptr, 3));
    ASSERT_EQ(tuple_list.size(), 3);
    EXPECT_EQ(std::get<0>(tuple_list.at(0)), k11);
    EXPECT_EQ(std::get<0>(tuple_list.at(1)), k12);
    EXPECT_EQ(std::get<0>(tuple_list.at(2)), k13);
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
