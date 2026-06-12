/**
 * @file interface_helper_test.cpp
 */

#include <array>

#include "test_tool.h"

#include "kvs.h"

using namespace std::string_view_literals;

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class interface_helper_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(interface_helper_test, destroy) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::array<std::string, 50> k{}; // NOLINT
    std::string v("value");
    std::size_t ctr{1};
    for (auto&& itr : k) {
        itr = std::string(1, ctr); // NOLINT
        ++ctr;
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, itr, v.data(), v.size()));
    }
    ASSERT_EQ(leave(token), status::OK);
    // destroy test by using destructor (fin());
}

TEST_F(interface_helper_test, init) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    ASSERT_NE(ti->load_root_ptr(), nullptr);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string k("a");
    std::string v("v-a");
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    ASSERT_NE(ti->load_root_ptr(), nullptr);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(interface_helper_test, mem_usage_iv) {
    // L0 (b)
    //     +-- L1_1 (b) - k11, k12
    //     +-- L1_2 (b) - k21, k22, k23
    auto k11 = "k1122334455"sv;
    auto k12 = "k112233445566"sv;
    auto k21 = "kAABBCCDDEE"sv;
    auto k22 = "kAABBCCDDEF"sv;
    auto k23 = "kAABBCCDDEG"sv;
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, test_storage_name, k11, &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, test_storage_name, k12, &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, test_storage_name, k21, &v2, sizeof(v2)));
    ASSERT_OK(put<void*>(token, test_storage_name, k22, &v2, sizeof(v2)));
    ASSERT_OK(put<void*>(token, test_storage_name, k23, &v2, sizeof(v2)));

    {
        tree_instance* ti{};
        ASSERT_OK(find_storage(test_storage_name, &ti));
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        border_node* b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_permutation_cnk(), 2);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k11).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        border_node* b11 = static_cast<border_node*>(b0->get_lv_at(0)->get_next_layer());
        ASSERT_EQ(b0->get_key_slice_at(1), base_node::key_tuple(k21).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(1)->get_next_layer()->get_version_border(), true);
        border_node* b12 = static_cast<border_node*>(b0->get_lv_at(1)->get_next_layer());
        ASSERT_EQ(b11->get_permutation_cnk(), 2);
        ASSERT_EQ(b12->get_permutation_cnk(), 3);
    }

    auto mem_stat = mem_usage(test_storage_name);
    ASSERT_EQ(mem_stat.size(), 2);
    EXPECT_EQ(mem_stat[0].bt_count, 1);
    EXPECT_EQ(mem_stat[0].in_count, 0);
    EXPECT_EQ(mem_stat[0].bn_count, 1);
    EXPECT_EQ(mem_stat[0].bn_allocated_mem, sizeof(border_node));
    EXPECT_EQ(mem_stat[0].bn_used_key, 2);
    EXPECT_EQ(mem_stat[0].iv_count, 0);
    EXPECT_EQ(mem_stat[1].bt_count, 2);
    EXPECT_EQ(mem_stat[1].bn_count, 2);
    EXPECT_EQ(mem_stat[1].bn_allocated_mem, sizeof(border_node) * 2);
    EXPECT_EQ(mem_stat[1].bn_used_key, 5);
    EXPECT_EQ(mem_stat[1].iv_count, 5);

    mem_usage_display(mem_stat);
    ASSERT_OK(leave(token));
}

TEST_F(interface_helper_test, mem_usage_vv) {
    // L0 (b)
    //     +-- L1_1 (b) - k11, k12
    //     +-- L1_2 (b) - k21, k22, k23
    auto k11 = "k1122334455"sv;
    auto k12 = "k112233445566"sv;
    auto k21 = "kAABBCCDDEE"sv;
    auto k22 = "kAABBCCDDEF"sv;
    auto k23 = "kAABBCCDDEG"sv;
    auto v4 = std::string(4, 'v');
    auto v64 = std::string(64, 'v');
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<char>(token, test_storage_name, k11, v4.data(), v4.size()));
    ASSERT_OK(put<char>(token, test_storage_name, k12, v4.data(), v4.size()));
    ASSERT_OK(put<char>(token, test_storage_name, k21, v64.data(), v64.size()));
    ASSERT_OK(put<char>(token, test_storage_name, k22, v64.data(), v64.size()));
    ASSERT_OK(put<char>(token, test_storage_name, k23, v64.data(), v64.size()));

    {
        tree_instance* ti{};
        ASSERT_OK(find_storage(test_storage_name, &ti));
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        border_node* b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_permutation_cnk(), 2);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k11).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        border_node* b11 = static_cast<border_node*>(b0->get_lv_at(0)->get_next_layer());
        ASSERT_EQ(b0->get_key_slice_at(1), base_node::key_tuple(k21).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(1)->get_next_layer()->get_version_border(), true);
        border_node* b12 = static_cast<border_node*>(b0->get_lv_at(1)->get_next_layer());
        ASSERT_EQ(b11->get_permutation_cnk(), 2);
        ASSERT_EQ(b12->get_permutation_cnk(), 3);
    }

    auto mem_stat = mem_usage(test_storage_name);
    ASSERT_EQ(mem_stat.size(), 2);
    EXPECT_EQ(mem_stat[0].bt_count, 1);
    EXPECT_EQ(mem_stat[0].in_count, 0);
    EXPECT_EQ(mem_stat[0].bn_count, 1);
    EXPECT_EQ(mem_stat[0].bn_allocated_mem, sizeof(border_node));
    EXPECT_EQ(mem_stat[0].bn_used_key, 2);
    EXPECT_EQ(mem_stat[0].vv_count, 0);
    EXPECT_EQ(mem_stat[0].vv_allocated_mem, 0);
    EXPECT_EQ(mem_stat[1].bt_count, 2);
    EXPECT_EQ(mem_stat[1].bn_count, 2);
    EXPECT_EQ(mem_stat[1].bn_allocated_mem, sizeof(border_node) * 2);
    EXPECT_EQ(mem_stat[1].bn_used_key, 5);
    EXPECT_EQ(mem_stat[1].vv_count, 5);
    EXPECT_EQ(mem_stat[1].vv_allocated_mem, (8 + 4) * 2 + (8 + 64) * 3);

    mem_usage_display(mem_stat);
    ASSERT_OK(leave(token));
}

} // namespace yakushima::testing
