/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>
#include <mutex>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class scan_long_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-scan-scan_long_key_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(scan_long_key_test, put_scan_delete) { // NOLINT
    // prepare
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::string st{"test"};
    ASSERT_EQ(status::OK, create_storage(st));

    std::string v{"v"};
    for (std::size_t i = 1; i < 1024 * 50; i *= 2) {
        // put
        LOG(INFO) << "test key size " << i << " bytes";
        std::string k(i, 'a');
        ASSERT_EQ(status::OK, put(s, st, k, v.data(), v.size()));

        // test: scan
        std::vector<std::tuple<std::string, char*, std::size_t>>
                tup_lis{}; // NOLINT
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "",
                                         scan_endpoint::INF, tup_lis, &nv));
        ASSERT_EQ(tup_lis.size(), 1);

        // delete
        ASSERT_EQ(status::OK, remove(s, st, std::string_view(k)));
    }

    // cleanup
    ASSERT_EQ(status::OK, leave(s));
}

} // namespace yakushima::testing