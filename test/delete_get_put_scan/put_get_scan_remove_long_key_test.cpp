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

class put_get_scan_remove_long_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-delete_get_put_scan-put_get_scan_remove_"
                "scan_remove_long_key_test");
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

TEST_F(put_get_scan_remove_long_key_test, put_get) { // NOLINT
                                                     // prepare
    Token s{};
    ASSERT_EQ(status::OK, enter(s));
    std::string st{"test"};
    ASSERT_EQ(status::OK, create_storage(st));

    std::string v{"v"};
    for (std::size_t i = 1; i < 1024 * 100; i *= 2) {
        LOG(INFO) << "test key size " << i << " bytes";
        std::string k(i, 'a');
        // test: put
        ASSERT_EQ(status::OK, put(s, st, k, v.data(), v.size()));

        // test: get
        std::pair<char*, std::size_t> tuple{};
        ASSERT_EQ(status::OK, get<char>(st, k, tuple));

        // verify
        std::string got(std::get<0>(tuple), std::get<1>(tuple));
        ASSERT_EQ(got, v);

        // todo scan operator test

        // delete
        ASSERT_EQ(status::OK, remove(s, st, std::string_view(k)));
    }

    // cleanup
    ASSERT_EQ(status::OK, leave(s));
}

} // namespace yakushima::testing