/**
 * @file
 * @brief tests for class iscan_context
 */

#include "test_tool.h"

#include "kvs.h"

using namespace yakushima;

using namespace std::literals;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

constexpr static bool suffix_enabled = true;

class iscan_context_test : public ::testing::Test {
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

template<typename T>
struct ctx_cleaner{
    ctx_cleaner(T*& p) : ptr(p) {}
    ~ctx_cleaner() { if (ptr) { delete ptr; } }
    T*& ptr;
};

TEST_F(iscan_context_test, calc_supremum_of_prefix) {
    std::string k = "\x12\x34"s;
    iscan_context::calc_supremum_of_prefix(k);
    EXPECT_EQ(k, "\x12\x35"s);

    k = "\x12\x34\xff"s;
    iscan_context::calc_supremum_of_prefix(k);
    EXPECT_EQ(k, "\x12\x35"s);

    k = "\x12\x34\xff\xff"s;
    iscan_context::calc_supremum_of_prefix(k);
    EXPECT_EQ(k, "\x12\x35"s);

    k = "\xff\xff"s;
    iscan_context::calc_supremum_of_prefix(k);
    EXPECT_EQ(k, ""s);
}

TEST_F(iscan_context_test, restart_point_child) {
    // L0 (b)
    //     +-- L1 (b) --- k1
    std::string k1("k123456789");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));
    if (suffix_enabled) {
        ASSERT_OK(put<void*>(token, st, k1 + " ", &v1, sizeof(v1)));
        ASSERT_OK(remove(token, st, k1 + " "));
    }

    border_node* b0;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k1).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(0)->get_next_layer()->get_version_border(), true);
    }

    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    void* val;
    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, false, ctx, val));
    ASSERT_EQ(ctx->stack_size(), 2);
    ASSERT_EQ(ctx->stack_top().key, base_node::key_tuple{"89"});
    ASSERT_EQ(ctx->full_key(), k1);
    // setup done

    // test
    auto restart_point = ctx->restart_point();
    EXPECT_EQ(restart_point.first, k1);
    EXPECT_EQ(restart_point.second, scan_endpoint::EXCLUSIVE);

    if (ctx) { ASSERT_OK(iscan_close(ctx)); }
    ASSERT_OK(leave(s));

}

TEST_F(iscan_context_test, restart_point_suffix_l1) {
    if (!suffix_enabled) { GTEST_SKIP() << "this test is for suffix-supported yakushima"; }
    // L0 (b)
    //     +-- L1 (b) --- k1
    //                 +- k2 (suffix)
    std::string k1("k123456789");
    std::string k2("k123456789ABCDEFGH");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, st, std::string_view(k2), &v2, sizeof(v2)));

    border_node* b0;
    border_node* b1;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k1).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        b1 = static_cast<border_node*>(b0->get_lv_at(0)->get_next_layer());
        ASSERT_EQ(b1->get_key_slice_at(0), base_node::key_tuple(k1.substr(8)).get_key_slice());
        ASSERT_EQ(b1->get_key_slice_at(1), base_node::key_tuple(k2.substr(8)).get_key_slice());
    }

    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    decltype(ctx->restart_point()) restart_point;
    void* val;

    //
    // forward scan
    //

    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, false, ctx, val));
    ASSERT_EQ(ctx->stack_size(), 2);
    ASSERT_EQ(ctx->stack_top().key, base_node::key_tuple{k1.substr(8)});
    ASSERT_EQ(ctx->full_key(), k1);

    restart_point = ctx->restart_point(); // at key
    EXPECT_EQ(restart_point.first, k1);
    EXPECT_EQ(restart_point.second, scan_endpoint::EXCLUSIVE);

    ASSERT_OK(iscan_next(ctx, val));
    ASSERT_EQ(ctx->full_key(), k2);

    restart_point = ctx->restart_point(); // at key with suffix
    EXPECT_GT(restart_point.first, k2);
    ASSERT_EQ(k2[15], 'F');
    EXPECT_EQ(restart_point.first, k2.substr(0, 15) + "G"); // 'F'+1
    EXPECT_EQ(restart_point.second, scan_endpoint::INCLUSIVE);

    // this breaks context !!
    // simulate just after returned to upper layer
    ctx->stack_pop();
    ASSERT_EQ(ctx->stack_size(), 1);

    restart_point = ctx->restart_point(); // at return from child
    EXPECT_GT(restart_point.first, k1);
    ASSERT_EQ(k1[7], '7');
    EXPECT_EQ(restart_point.first, k1.substr(0, 7) + "8"); // '7'+1
    EXPECT_EQ(restart_point.second, scan_endpoint::INCLUSIVE);

    // this breaks context !!
    // simulate just after moved to lower layer
    ctx->stack(base_node::key_tuple::min(),
               b1, b1, -1, ctx->stack_top().bi); // dummy values

    restart_point = ctx->restart_point(); // at perm iteration start
    EXPECT_EQ(restart_point.first, k1.substr(0, 8));
    EXPECT_EQ(restart_point.second, scan_endpoint::EXCLUSIVE);

    ASSERT_OK(iscan_close(ctx));

    //
    // reverse scan
    //

    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, false, ctx, val));
    ASSERT_EQ(ctx->stack_size(), 2);
    ASSERT_EQ(ctx->stack_top().key, base_node::key_tuple{k2.substr(8)});
    ASSERT_EQ(ctx->full_key(), k2);

    restart_point = ctx->restart_point(); // at key with suffix
    EXPECT_LT(restart_point.first, k2);
    EXPECT_EQ(restart_point.first, k2.substr(0, 16));
    EXPECT_EQ(restart_point.second, scan_endpoint::INCLUSIVE);

    ASSERT_OK(iscan_next(ctx, val));
    ASSERT_EQ(ctx->full_key(), k1);

    restart_point = ctx->restart_point(); // at key
    EXPECT_EQ(restart_point.first, k1);
    EXPECT_EQ(restart_point.second, scan_endpoint::EXCLUSIVE);

    // this breaks context !!
    ctx->stack_pop();
    ASSERT_EQ(ctx->stack_size(), 1);

    restart_point = ctx->restart_point(); // at return from child
    EXPECT_LT(restart_point.first, k1);
    EXPECT_EQ(restart_point.first, k1.substr(0, 8));
    EXPECT_EQ(restart_point.second, scan_endpoint::INCLUSIVE);

    // this breaks context !!
    // simulate just after moved to lower layer
    ctx->stack(base_node::key_tuple::max(),
               b1, b1, 0, ctx->stack_top().bi); // dummy values

    restart_point = ctx->restart_point(); // at perm iteration start
    EXPECT_GT(restart_point.first, k2);
    ASSERT_EQ(k2[7], '7');
    EXPECT_EQ(restart_point.first, k2.substr(0, 7) + "8"); // '7'+1
    EXPECT_EQ(restart_point.second, scan_endpoint::EXCLUSIVE);

    ASSERT_OK(iscan_close(ctx));

    ASSERT_OK(leave(s));
}

TEST_F(iscan_context_test, restart_point_suffix_l0) {
    if (!suffix_enabled) { GTEST_SKIP() << "this test is for suffix-supported yakushima"; }
    std::string k1(10U, '\xff');
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));

    border_node* b0;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k1).get_key_slice());
    }

    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    decltype(ctx->restart_point()) restart_point;
    void* val;

    //
    // forward scan
    //

    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, false, ctx, val));
    ASSERT_EQ(ctx->stack_size(), 1);
    ASSERT_EQ(ctx->stack_top().key, base_node::key_tuple{k1});
    ASSERT_EQ(ctx->full_key(), k1);

    restart_point = ctx->restart_point(); // at key with suffix, covered to +inf
    EXPECT_EQ(restart_point.second, scan_endpoint::INF);

    ASSERT_OK(iscan_close(ctx));

    ASSERT_OK(leave(s));
}

} // namespace yakushima::testing
