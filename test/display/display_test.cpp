/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>
#include <mutex>
#include <string>

#include "kvs.h"

// yakushima/test/include
#include "test_tool.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class display_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-display-display_test");
        // FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { init(); }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(display_test, simple) { // NOLINT
    // prepare
    // create storage
    std::string st1{"test1"};
    ASSERT_OK(create_storage(st1));
    std::string st2{"test2"};
    ASSERT_OK(create_storage(st2));
    std::string st3{"test3"};
    ASSERT_OK(create_storage(st3));
    std::string st4{"test4"};
    ASSERT_OK(create_storage(st4));
    std::string st5{"test5"};
    ASSERT_OK(create_storage(st5));
    std::string st6{"test6"};
    ASSERT_OK(create_storage(st6));
    Token t{};
    ASSERT_OK(enter(t));

    // put key value
    std::string k("k1");
    std::string v{"v1"};
    yakushima::node_version64* nvp_for_put{};
    char* tmp_created_value_ptr{};

    // test1: 1 border 1 entry
    ASSERT_OK(put(t, st1, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true,
                  &nvp_for_put));

    // test2: 1 border 2 entry
    k = "k2";
    v = "v2"; // inline opt
    ASSERT_OK(put(t, st2, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true,
                  &nvp_for_put));
    k = "k3";
    v = "123456789"; // not inline opt
    ASSERT_OK(put(t, st2, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true,
                  &nvp_for_put));

    // test3: multi layer 2 border
    k = "12345678a";
    v = "v";
    ASSERT_OK(put(t, st3, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true,
                  &nvp_for_put));

    // test4: 1 interior 2 border
    for (std::size_t i = 0; i < 20; ++i) {
        k = std::to_string(i);
        ASSERT_OK(put(t, st4, k, v.data(), v.size(), &tmp_created_value_ptr,
                      static_cast<value_align_type>(alignof(char)), true,
                      &nvp_for_put));
    }

    // test5: multi layer version of test4
    for (std::size_t i = 0; i < 20; ++i) {
        k = "abcdefgh" + std::to_string(i);
        ASSERT_OK(put(t, st5, k, v.data(), v.size(), &tmp_created_value_ptr,
                      static_cast<value_align_type>(alignof(char)), true,
                      &nvp_for_put));
    }

    // test6: multi layer 2 border (inlined)
    k = "12345678a";
    auto* p = reinterpret_cast<void*>(uintptr_t(0x12345678AULL)); // NOLINT
    ASSERT_OK(put<void*>(t, st6, k, &p, sizeof(p)));

    // test
    display();

    // cleanup
    ASSERT_OK(leave(t));
}

TEST_F(display_test, binary) { // NOLINT
    using namespace std::literals::string_literals;

    std::string st{"test"};
    ASSERT_OK(create_storage(st));
    Token t{};
    ASSERT_OK(enter(t));

    std::string data[][2] = { // NOLINT
        {"k1"s, "v1"s},
        {"\0\0"s, "\0\0\0\0"s},
        {"\0\0\0\0\0\0\0\0\0"s, "NUL9"s}
    };
    for (auto& [k, v] : data) {
        ASSERT_OK(put(t, st, k, v.data(), v.size())) << "k=" << k;
    }

    display();

    ASSERT_OK(leave(t));
}

} // namespace yakushima::testing
