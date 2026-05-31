/**
 * @file
 * @brief tests for iscan functions, behavior on detecting concurrent modifications
 */

#include <array>
#include <chrono>
#include <random>
#include <thread>

#include "test_tool.h"

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

using namespace std::literals;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class iscan_concurrent_modify_test : public ::testing::Test {
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

TEST_F(iscan_concurrent_modify_test, insert_same_border_scanned) {
    // layer 0 border 1
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000001110808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000002220808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, "k", &v1, sizeof(v1)));

    std::vector<void*> tup_lis{}; // NOLINT
    int state = 0;
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    auto cb = [&](node_version64*, node_version64_body) {
        if (state == 0) {
            Token s2;
            enter(s2);
            put<void*>(s2, st, "a", &v2, sizeof(v2)); // scanned area
            leave(s2);
            state = 1;
        }
        return false;
    };
    void* val;

    // (-inf, +inf)  full-scan
    ASSERT_EQ(status::OK,
              iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
              false, true, ctx, val, cb));
    ASSERT_EQ(val, v1);
    ASSERT_EQ(status::WARN_CONCURRENT_OPERATIONS, iscan_next(ctx, val, cb));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_OK(leave(token));
}

TEST_F(iscan_concurrent_modify_test, concurrent_insert_same_border_unscanned) {
    // layer 0 border 1
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000001110808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000002220808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, "k", &v1, sizeof(v1)));

    std::vector<void*> tup_lis{}; // NOLINT
    int state = 0;
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    auto cb = [&](node_version64*, node_version64_body) {
        if (state == 0) {
            Token s2;
            enter(s2);
            put<void*>(s2, st, "m", &v2, sizeof(v2)); // unscanned, but same border
            leave(s2);
            state = 1;
        }
        return false;
    };
    void* val;

    // (-inf, +inf)  full-scan
    ASSERT_EQ(status::OK,
              iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
              false, true, ctx, val, cb));
    ASSERT_EQ(val, v1);
    // false positive
    ASSERT_EQ(status::WARN_CONCURRENT_OPERATIONS, iscan_next(ctx, val, cb));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_OK(leave(token));
}

TEST_F(iscan_concurrent_modify_test, insert_unscanned_border) {
    // layer0 2borders
    // L0 (b1-b2)
    std::string k1("k1"); // lowkey
    std::string k2("k2"); // lowkey
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    void* v3 = reinterpret_cast<void*>(uintptr_t(0x0000000083808080));
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, k1, &v1, sizeof(v1)));
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(put<void*>(token, st, "k1" + std::to_string(i), &v, sizeof(v)));
    }
    ASSERT_OK(put<void*>(token, st, k2, &v2, sizeof(v2)));
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(put<void*>(token, st, "k2" + std::to_string(i), &v, sizeof(v)));
    }
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(remove(token, st, "k2" + std::to_string(i)));
        ASSERT_OK(remove(token, st, "k1" + std::to_string(i)));
    }

    border_node* b1;
    border_node* b2;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), false);
        ASSERT_EQ(root->get_version_deleted(), false);
        auto* i = static_cast<interior_node*>(root);
        ASSERT_EQ(i->get_n_keys(), 1);
        ASSERT_EQ(i->get_child_at(0)->get_version_border(), true);
        b1 = static_cast<border_node*>(i->get_child_at(0));
        ASSERT_EQ(i->get_child_at(1)->get_version_border(), true);
        b2 = static_cast<border_node*>(i->get_child_at(1));
    }

    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    std::vector<node_version64*> nvps{};
    auto cb = [&nvps](node_version64* nvp, node_version64_body) {
        VLOG(log_debug) << "cb p:" << nvp;
        nvps.emplace_back(nvp);
        return false;
    };
    void* val;

    // (-inf, +inf)  full-scan
    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                         false, true, ctx, val, cb));
    EXPECT_EQ(val, v1);
    EXPECT_EQ(nvps, std::vector<node_version64*>({ b1->get_version_ptr() }));
    nvps.clear();
    {
        VLOG(log_debug) << "remove";
        Token s2{};
        enter(s2);
        put<void*>(s2, st, "m", &v3, sizeof(v3)); // right border
        leave(s2);
    }
    ASSERT_EQ(status::OK, iscan_next(ctx, val, cb));
    EXPECT_EQ(val, v2);
    EXPECT_EQ(nvps, std::vector<node_version64*>({ b1->get_version_ptr(), b2->get_version_ptr() }));
    nvps.clear();
    ASSERT_EQ(status::OK, iscan_next(ctx, val, cb));
    EXPECT_EQ(val, v3);
    EXPECT_EQ(nvps, std::vector<node_version64*>({ b2->get_version_ptr() }));
    nvps.clear();
    ASSERT_EQ(status::OK_SCAN_END, iscan_next(ctx, val, cb));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_OK(leave(token));
}

TEST_F(iscan_concurrent_modify_test, remove_unscanned_border) {
    // layer0 3borders
    // L0 (b1-b2-b3)
    std::string k1("k1");
    std::string k2("k2");
    std::string k3("k3");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    void* v3 = reinterpret_cast<void*>(uintptr_t(0x0000000083808080));
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, k1, &v1, sizeof(v1)));
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(put<void*>(token, st, "k1" + std::to_string(i), &v, sizeof(v)));
    }
    ASSERT_OK(put<void*>(token, st, k2, &v2, sizeof(v2)));
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(put<void*>(token, st, "k2" + std::to_string(i), &v, sizeof(v)));
    }
    ASSERT_OK(put<void*>(token, st, k3, &v3, sizeof(v2)));
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(put<void*>(token, st, "k3" + std::to_string(i), &v, sizeof(v)));
    }
    for (int i = 1; i < 8; i++) {
        ASSERT_OK(remove(token, st, "k3" + std::to_string(i)));
        ASSERT_OK(remove(token, st, "k2" + std::to_string(i)));
        ASSERT_OK(remove(token, st, "k1" + std::to_string(i)));
    }

    border_node* b1;
    [[maybe_unused]] border_node* b2;
    border_node* b3;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), false);
        ASSERT_EQ(root->get_version_deleted(), false);
        auto* i = static_cast<interior_node*>(root);
        ASSERT_EQ(i->get_n_keys(), 2);
        ASSERT_EQ(i->get_child_at(0)->get_version_border(), true);
        b1 = static_cast<border_node*>(i->get_child_at(0));
        ASSERT_EQ(i->get_child_at(1)->get_version_border(), true);
        b2 = static_cast<border_node*>(i->get_child_at(1));
        ASSERT_EQ(i->get_child_at(2)->get_version_border(), true);
        b3 = static_cast<border_node*>(i->get_child_at(2));
    }

    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    std::vector<node_version64*> nvps{};
    auto cb = [&nvps](node_version64* nvp, node_version64_body) {
        VLOG(log_debug) << "cb p:" << nvp;
        nvps.emplace_back(nvp);
        return false;
    };
    void* val;

    // (-inf, +inf)  full-scan
    ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
                         false, true, ctx, val, cb));
    ASSERT_EQ(val, v1);
    EXPECT_EQ(nvps, std::vector<node_version64*>({ b1->get_version_ptr() }));
    nvps.clear();
    {
        VLOG(log_debug) << "remove";
        Token s2{};
        enter(s2);
        remove(s2, st, k2);
        leave(s2);
    }
    ASSERT_EQ(status::OK, iscan_next(ctx, val, cb));
    ASSERT_EQ(val, v3);
    EXPECT_EQ(nvps, std::vector<node_version64*>({ b1->get_version_ptr(), b3->get_version_ptr() }));
    nvps.clear();
    ASSERT_EQ(status::OK_SCAN_END, iscan_next(ctx, val, cb));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_OK(leave(token));
}

TEST_F(iscan_concurrent_modify_test, abort_by_user) {
    // layer 0 border 1
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000001110808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, "k", &v1, sizeof(v1)));

    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    auto cbt = [] (node_version64*, node_version64_body) { return true; };
    auto cbf = [] (node_version64*, node_version64_body) { return false; };
    void* val;

    ASSERT_EQ(status::WARN_ABORTED_BY_USER,
              iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
              false, false, ctx, val, cbt));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_EQ(status::OK,
              iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF,
              false, false, ctx, val, cbf));
    ASSERT_EQ(val, v1);
    ASSERT_EQ(status::WARN_ABORTED_BY_USER, iscan_next(ctx, val, cbt));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }

    ASSERT_OK(leave(token));
}

} // namespace yakushima::testing
