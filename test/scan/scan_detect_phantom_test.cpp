/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>
#include <mutex>

#include "kvs.h"

#include "glog/logging.h"

#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class scan_detect_phantom_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-scan-scan_detect_phantom_test");
        FLAGS_stderrthreshold = 0;
    }
    void SetUp() override {
        init();
        create_storage(st);
        std::call_once(init_, call_once_f);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(scan_detect_phantom_test,               // NOLINT
       scan_put_delete_check_for_empty_tree) { // NOLINT
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "",
                                     scan_endpoint::INF, tup_lis, &nv));
    // check nv is not empty and size is one.
    ASSERT_FALSE(nv.empty());
    ASSERT_EQ(nv.size(), 1);
    // get nvp from nv
    auto nvp = nv.front();

    std::string v{"v"};
    ASSERT_EQ(status::OK, put(s, st, "", v.data(), v.size()));
    // check phantom
    auto nv2 = nvp.second->get_stable_version();
    ASSERT_NE(nvp.first, nv2);
    ASSERT_EQ(status::OK, remove(s, st, ""));
    // check phantom
    auto nv3 = nvp.second->get_stable_version();
    ASSERT_NE(nvp.first, nv3);
    ASSERT_NE(nv2, nv3);
    ASSERT_EQ(status::OK, leave(s));
}

TEST_F(scan_detect_phantom_test,                   // NOLINT
       scan_put_delete_check_for_not_empty_tree) { // NOLINT
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(s, st, "a", v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_EQ(status::OK, scan<char>(st, "", scan_endpoint::INF, "",
                                     scan_endpoint::INF, tup_lis, &nv));
    // check nv is not empty and size is one.
    ASSERT_FALSE(nv.empty());
    ASSERT_EQ(nv.size(), 1);
    // get nvp from nv
    auto nvp = nv.front();

    ASSERT_EQ(status::OK, put(s, st, "", v.data(), v.size()));
    // check phantom
    auto nv2 = nvp.second->get_stable_version();
    ASSERT_NE(nvp.first, nv2);
    ASSERT_EQ(status::OK, remove(s, st, ""));
    // check not phantom about remove
    auto nv3 = nvp.second->get_stable_version();
    ASSERT_EQ(nv2, nv3);
    ASSERT_EQ(status::OK, leave(s));
}

} // namespace yakushima::testing