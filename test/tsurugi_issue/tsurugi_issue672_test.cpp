/**
 * @file scan_basic_usage_test.cpp
 */

#include <array>
#include <mutex>

#include "kvs.h"

// yakushima/test/include
#include "test_tool.h"

#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class tsurugi_issue672_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-tsurugi_issue-tsurugi_issue672_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override { init(); }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

TEST_F(tsurugi_issue672_test, simple) { // NOLINT
    // prepare
    std::string st{"test"};
    ASSERT_OK(create_storage(st));

    Token t{};
    ASSERT_OK(enter(t));

    // test
    std::vector<std::tuple<std::string, char*, std::size_t>> tup{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    ASSERT_OK(scan<char>(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                         tup, &nv));

    // verify for scan against no entry
    ASSERT_EQ(tup.size(), 0);
    ASSERT_EQ(nv.size(), 1);
    tree_instance* ti{};
    find_storage(st, &ti);
    auto* n = ti->load_root_ptr();
    auto* nvp = n->get_version_ptr();
    ASSERT_EQ(nvp, std::get<1>(*nv.begin()));

    // insert 1 key len9
    std::string k(9, '0');
    std::string v{"v"};
    yakushima::node_version64* nvp_for_put{};
    char* tmp_created_value_ptr{};
    ASSERT_OK(put(t, st, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true,
                  &nvp_for_put));

    // verify
    auto* nvp_first_border_node = n->get_version_ptr();
    ASSERT_EQ(nvp_for_put, nvp_first_border_node);
    auto* n_second_border_node =
            dynamic_cast<border_node*>(n)->get_lv_at(0)->get_next_layer();
    ASSERT_NE(nvp_for_put, n_second_border_node->get_version_ptr());

    // cleanup
    ASSERT_OK(leave(t));
}

} // namespace yakushima::testing
