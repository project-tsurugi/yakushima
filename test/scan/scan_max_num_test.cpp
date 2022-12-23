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
} // namespace yakushima::testing
