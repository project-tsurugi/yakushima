/**
 * @file scan_test.cpp
 */

#include <array>

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

TEST_F(st, scan_three_border_one_border) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    for (char i = 0; i <= 25; ++i) { // NOLINT
        char c = i;
        ASSERT_EQ(status::OK, put(token, test_storage_name,
                                  std::string_view(&c, 1), v.data(), v.size()));
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
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF,
                                     "", scan_endpoint::INF, tup, &nv));
    auto delete_range = [&token](char begin, char end) {
        for (char i = begin; i <= end; ++i) {
            char c = i;
            ASSERT_EQ(status::OK, remove(token, test_storage_name,
                                         std::string_view(&c, 1)));
        }
    };
    delete_range(1, 7);   // NOLINT
    delete_range(15, 15); // NOLINT
    delete_range(16, 24); // NOLINT
    /**
   * now
   * A:  0,
   * B:  8, 9, 10, 11, 12, 13, 14,
   * C:  25
   * branch of A and B is 8
   * branch of B and C is 16
   */
    char begin = 1; // NOLINT
    char end = 24;  // NOLINT
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, std::string_view(&begin, 1),
                         scan_endpoint::INCLUSIVE, std::string_view(&end, 1),
                         scan_endpoint::INCLUSIVE, tup, &nv));
    ASSERT_EQ(tup.size(), 7); // NOLINT
    ASSERT_NE(nv.at(0), nv.at(1));
    ASSERT_EQ(nv.at(1), nv.at(2));
    ASSERT_NE(nv.at(nv.size() - 1), nv.at(nv.size() - 2));
    ASSERT_EQ(nv.size(), tup.size() + 2); // NOLINT
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
