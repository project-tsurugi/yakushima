/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>
#include <mutex>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class tsurugi_issue251_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-tsurugi_issue-tsurugi_issue251_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { init(); }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue251_test, 20230426_comment_ban) { // NOLINT
    // prepare
    constexpr int rec_num{100000};
    std::string st{"st"};
    ASSERT_EQ(status::OK, create_storage(st));

    std::atomic_bool finish_insert{false};
    auto work_insert = [st, rec_num, &finish_insert]() {
        Token token{};
        ASSERT_EQ(status::OK, enter(token));
        for (int i = 0; i < rec_num; ++i) {
            char buf[32];
            sprintf(buf, "%012d", i);
            ASSERT_EQ(status::OK,
                      put(token, st, std::string_view(buf), &i, sizeof(i)));
        }
        finish_insert = true;
        // cleanup
        ASSERT_EQ(status::OK, leave(token));
    };

    auto work_scan = [st, rec_num, &finish_insert]() {
        Token token{};
        ASSERT_EQ(status::OK, enter(token));
        while (!finish_insert) {
            std::vector<std::tuple<std::string, int*, std::size_t>> tuple_list;
            ASSERT_EQ(tuple_list.size(), 0);
            ASSERT_EQ(status::OK,
                      scan(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                           tuple_list, nullptr, 0));
            auto size = tuple_list.size();
            for (std::size_t j = 1; j < size; j++) {
                if (std::get<0>(tuple_list[j - 1]) >=
                    std::get<0>(tuple_list[j])) {
                    std::string str_j_1 = std::get<0>(tuple_list[j - 1]);
                    std::string str_j = std::get<0>(tuple_list[j]);
                    LOG(FATAL) << "j-1:" << j - 1 << ", " << str_j_1
                               << ", j:" << j << ", " << str_j;
                }
            }
        }
        ASSERT_EQ(status::OK, leave(token));
    };

    std::thread ins_th(work_insert);
    std::thread scan_th(work_scan);

    ins_th.join();
    scan_th.join();
}

} // namespace yakushima::testing