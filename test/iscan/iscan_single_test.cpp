/**
 * @file
 * @brief tests for iscan functions, without concurrent modifications
 */

// [limitation-from-borders-dont-now-lowkey-highkey]:
//   Occasionally, more callbacks occur than ideally. This is because,
//   in current yakushima implementation, border nodes don't know their own lowkey and highkey,
//   so the iterator cannot realize it is outside the scan range until it moves to neighboring node.

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

class iscan_single_test : public ::testing::Test {
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

TEST_F(iscan_single_test, at_non_existing_storage) {
    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    EXPECT_EQ(status::WARN_STORAGE_NOT_EXIST,
              iscan_open("nowhere", "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, false, true, ctx, v));
    EXPECT_EQ(ctx, nullptr);
    ASSERT_OK(leave(s));
}

TEST_F(iscan_single_test, invalid_range) {
    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    EXPECT_EQ(status::ERR_BAD_USAGE,
              iscan_open(st, "2", scan_endpoint::INCLUSIVE, "1",
                         scan_endpoint::INCLUSIVE, false, true, ctx, v));
    EXPECT_EQ(ctx, nullptr);
    ASSERT_OK(leave(s));
}

TEST_F(iscan_single_test, empty_alive_empty_border) {
    // just after create_storage, no documentation about this state
    // current implementation: root is size-0 alive border node
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        border_node* bn = static_cast<border_node*>(root);
        ASSERT_EQ(bn->get_permutation_cnk(), 0);
    }

    Token s{};
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    void* val;
    EXPECT_EQ(status::OK_SCAN_END,
              iscan_open(st, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, false, true, ctx, val));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }
    ASSERT_OK(leave(s));
}

TEST_F(iscan_single_test, empty_deleted_border) {
    // deleted root border
    Token s{};
    ASSERT_OK(enter(s));
    std::string_view k("k");
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    ASSERT_OK(put<void*>(s, st, k, &v, sizeof(v)));
    ASSERT_OK(remove(s, st, k));
    ASSERT_OK(leave(s));
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), true);
    }
    ASSERT_OK(enter(s));
    iscan_context* ctx{nullptr};
    void* val;
    EXPECT_EQ(status::OK_SCAN_END,
              iscan_open(st, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, false, true, ctx, val));
    if (ctx) { ASSERT_OK(iscan_close(ctx)); }
    ASSERT_OK(leave(s));
}

using B = std::vector<base_node*>;

std::vector<std::variant<B, void*>> rev_expected(const std::vector<std::variant<B, void*>>& exp) {
    std::vector<std::variant<B, void*>> ret{};
    ret.reserve(exp.size());
    for (auto itr = exp.crbegin(); itr != exp.crend(); itr++) {
        if (std::holds_alternative<void*>(*itr)) {
            ret.emplace_back(*itr);
        } else {
            auto&& expected_bns = std::get<0>(*itr);
            B revb(expected_bns.size());
            std::reverse_copy(expected_bns.begin(), expected_bns.end(), revb.begin());
            ret.emplace_back(revb);
        }
    }
    return ret;
}

void tc(std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
        bool right_to_left,
        std::vector<std::variant<B, void*>>&& expected) {
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    void* val;
    std::vector<node_version64*> nvps{};
    auto cb = [&nvps](node_version64* nvp, node_version64_body) {
        VLOG(log_debug) << "cb p:" << nvp;
        nvps.emplace_back(nvp);
        return false;
    };
    std::stringstream rangess{};
    rangess << "range:";
    if (l_end == scan_endpoint::INF) { rangess << "(-inf"; }
    else { rangess << (l_end == scan_endpoint::INCLUSIVE ? '[' : '(') << '"' << l_key << '"'; }
    rangess << ", ";
    if (r_end == scan_endpoint::INF) { rangess << "+inf)"; }
    else { rangess << '"' << r_key << '"' << (r_end == scan_endpoint::INCLUSIVE ? ']' : ')'); }
    rangess << " " << (right_to_left ? "reverse" : "forward") << "-order";
    auto range = rangess.str();
    ASSERT_TRUE(expected.size() % 2 == 1) << range << "\nexpected must be {bns, v, bns, v, ..., bns},"
                                          << " so size must be odd number. size=" << expected.size();
    for (std::size_t i = 0; i + 1 < expected.size(); i+=2) {
        ASSERT_FALSE(std::holds_alternative<void*>(expected[i]))
            << range << "\nexpected must be {bns, v, bns, v, ..., bns}. index=" << i;
        ASSERT_TRUE(std::holds_alternative<void*>(expected[i + 1]))
            << range << "\nexpected must be {bns, v, bns, v, ..., bns}. index=" << i + 1;
    }
    ASSERT_FALSE(std::holds_alternative<void*>(expected.back()))
        << range << "\nexpected must be {bns, v, bns, v, ..., bns}";
    if (expected.size() == 1) {
        nvps.clear();
        ASSERT_EQ(status::OK_SCAN_END,
                  iscan_open(st, l_key, l_end, r_key, r_end, right_to_left, false, ctx, val, cb)) << range;
        auto&& expected_bns = std::get<0>(expected[0]);
        EXPECT_EQ(nvps.size(), expected_bns.size()) << range;
        if (nvps.size() == expected_bns.size()) {
            for (std::size_t j = 0; j < nvps.size(); j++) {
                EXPECT_EQ(nvps[j], expected_bns[j]->get_version_ptr()) << range << " j:" << j;
            }
        }
        if (ctx) { ASSERT_OK(iscan_close(ctx)); }
        return;
    }
    {
        nvps.clear();
        ASSERT_EQ(status::OK,
                  iscan_open(st, l_key, l_end, r_key, r_end, right_to_left, false, ctx, val, cb)) << range;
        auto&& expected_bns = std::get<0>(expected[0]);
        EXPECT_EQ(nvps.size(), expected_bns.size()) << range;
        if (nvps.size() == expected_bns.size()) {
            for (std::size_t j = 0; j < nvps.size(); j++) {
                EXPECT_EQ(nvps[j], expected_bns[j]->get_version_ptr()) << range << " j:" << j;
            }
        }
        EXPECT_EQ(val, std::get<1>(expected[1])) << range;
    }
    for (std::size_t i = 2; i + 1 < expected.size(); i+=2) {
        nvps.clear();
        ASSERT_EQ(status::OK, iscan_next(ctx, val, cb)) << range;
        auto&& expected_bns = std::get<0>(expected[i]);
        EXPECT_EQ(nvps.size(), expected_bns.size()) << range;
        if (nvps.size() == expected_bns.size()) {
            for (std::size_t j = 0; j < nvps.size(); j++) {
                EXPECT_EQ(nvps[j], expected_bns[j]->get_version_ptr()) << range << " i:" << i << " j:" << j;
            }
        }
        EXPECT_EQ(val, std::get<1>(expected[i + 1])) << range << " i:" << i+1;
    }
    {
        nvps.clear();
        ASSERT_EQ(status::OK_SCAN_END, iscan_next(ctx, val, cb)) << range;
        auto&& expected_bns = std::get<0>(expected.back());
        EXPECT_EQ(nvps.size(), expected_bns.size()) << range;
        if (nvps.size() == expected_bns.size()) {
            for (std::size_t j = 0; j < nvps.size(); j++) {
                EXPECT_EQ(nvps[j], expected_bns[j]->get_version_ptr()) << range << " j:" << j;
            }
        }
    }
    ASSERT_OK(iscan_close(ctx));
}

void tc(std::string_view l_key, scan_endpoint l_end, std::string_view r_key, scan_endpoint r_end,
        std::vector<std::variant<B, void*>>&& expected) {
    auto rexp = rev_expected(expected); // make reverse copy
    tc(l_key, l_end, r_key, r_end, false, std::move(expected));
    tc(l_key, l_end, r_key, r_end, true, std::move(rexp));
}

TEST_F(iscan_single_test, l0b1) {
    // layer 0 border 1
    std::string k("k");
    void* v = reinterpret_cast<void*>(uintptr_t(0x0000000080808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k), &v, sizeof(v)));

    border_node* b;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        b = static_cast<border_node*>(root);
    }

    // ["1", "1"]
    tc("1", scan_endpoint::INCLUSIVE, "1", scan_endpoint::INCLUSIVE,
       {B{b}});

    // ["", ""]
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE,
       {B{b}});

    // (-inf, +inf)  full-scan
    tc("", scan_endpoint::INF, "", scan_endpoint::INF,
       {B{b}, v, B{b}});

    // (-inf, ""] => ["", ""]
    tc("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE,
       {B{b}});

    // ["", +inf)  full-scan
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b}, v, B{b}});

    // ("", +inf)
    tc("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b}, v, B{b}});

    // (-inf, "k"]
    tc("", scan_endpoint::INF, "k", scan_endpoint::INCLUSIVE,
       {B{b}, v, B{}});

    // (-inf, "k")
    tc("", scan_endpoint::INF, "k", scan_endpoint::EXCLUSIVE,
       {B{b}});

    // ["j", "l"]
    tc("j", scan_endpoint::INCLUSIVE, "l", scan_endpoint::INCLUSIVE,
       {B{b}, v, B{b}});

    // ["j123456789", "l123456789"]
    tc("j123456789", scan_endpoint::INCLUSIVE, "l123456789", scan_endpoint::INCLUSIVE,
       {B{b}, v, B{b}});

    // ["k", "k"]
    tc("k", scan_endpoint::INCLUSIVE, "k", scan_endpoint::INCLUSIVE,
       {B{}, v, B{}});

    // ["k", "l"]
    tc("k", scan_endpoint::INCLUSIVE, "l", scan_endpoint::INCLUSIVE,
       {B{}, v, B{b}});

    // ["k", "l123456789"]
    tc("k", scan_endpoint::INCLUSIVE, "l123456789", scan_endpoint::INCLUSIVE,
       {B{}, v, B{b}});

    // ["k", "l123456789")
    tc("k", scan_endpoint::INCLUSIVE, "l123456789", scan_endpoint::EXCLUSIVE,
       {B{}, v, B{b}});
}

TEST_F(iscan_single_test, l0b2_lowkey) {
    // layer0 2borders
    // L0 (b01-b02)
    std::string k1("k1"); // lowkey
    std::string k2("k2"); // lowkey
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
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

    border_node* b01;
    border_node* b02;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), false);
        ASSERT_EQ(root->get_version_deleted(), false);
        auto* i = static_cast<interior_node*>(root);
        ASSERT_EQ(i->get_n_keys(), 1);
        ASSERT_EQ(i->get_child_at(0)->get_version_border(), true);
        b01 = static_cast<border_node*>(i->get_child_at(0));
        ASSERT_EQ(i->get_child_at(1)->get_version_border(), true);
        b02 = static_cast<border_node*>(i->get_child_at(1));
    }

    // ["", ""]
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE,
       {B{b01}});

    // (-inf, +inf)  full-scan
    tc("", scan_endpoint::INF, "", scan_endpoint::INF,
       {B{b01}, v1, B{b01, b02}, v2, B{b02}});

    // (-inf, ""] => ["", ""]
    tc("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE,
       {B{b01}});

    // ["", +inf)  full-scan
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b01}, v1, B{b01, b02}, v2, B{b02}});

    // ("", +inf)
    tc("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b01}, v1, B{b01, b02}, v2, B{b02}});

    // (-inf, "k1"]
    tc("", scan_endpoint::INF, "k1", scan_endpoint::INCLUSIVE,
       {B{b01}, v1, B{}});

    // (-inf, "k1")
    tc("", scan_endpoint::INF, "k1", scan_endpoint::EXCLUSIVE,
       {B{b01}});

    // ["k0", "k19"]
    tc("k0", scan_endpoint::INCLUSIVE, "k19", scan_endpoint::INCLUSIVE, false,
       {B{b01},
        v1,
        //B{b01} // ideal
        B{b01, b02} // limitation-from-borders-dont-now-lowkey-highkey
    });
    tc("k0", scan_endpoint::INCLUSIVE, "k19", scan_endpoint::INCLUSIVE, true,
       {B{b01}, v1, B{b01}});

    // ["k11", "k2"]
    tc("k11", scan_endpoint::INCLUSIVE, "k2", scan_endpoint::INCLUSIVE,
       {B{b01, b02}, v2, B{}}
      );

    // ["k11", "k2")
    tc("k11", scan_endpoint::INCLUSIVE, "k2", scan_endpoint::EXCLUSIVE,
       //{B{b01} // ideal
       {B{b01, b02}} // limitation-from-borders-dont-now-lowkey-highkey
      );

    ASSERT_OK(leave(token));
}

TEST_F(iscan_single_test, l1b1b1) {
    // L0 (b)
    //     +-- L11 (b) --- k1
    //     +-- L12 (b) --- k2
    std::string k1("k123456789");
    std::string k2("k23456789");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, st, std::string_view(k2), &v2, sizeof(v2)));

    border_node* b0;
    border_node* b11;
    border_node* b12;
    {
        tree_instance* ti{};
        find_storage(st, &ti);
        base_node* root = ti->load_root_ptr();
        ASSERT_EQ(root->get_version_border(), true);
        ASSERT_EQ(root->get_version_deleted(), false);
        b0 = static_cast<border_node*>(root);
        ASSERT_EQ(b0->get_key_slice_at(0), base_node::key_tuple(k1).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        b11 = static_cast<border_node*>(b0->get_lv_at(0)->get_next_layer());
        ASSERT_EQ(b0->get_key_slice_at(1), base_node::key_tuple(k2).get_key_slice());
        ASSERT_EQ(b0->get_lv_at(1)->get_next_layer()->get_version_border(), true);
        b12 = static_cast<border_node*>(b0->get_lv_at(1)->get_next_layer());
    }

    // ["", ""]
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE,
       {B{b0}});

    // (-inf, +inf)  full-scan
    tc("", scan_endpoint::INF, "", scan_endpoint::INF,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});

    // (-inf, ""] => ["", ""]
    tc("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE,
       {B{b0}});
    // (-inf, "k")  ("k" is less than k1 in L0)
    tc("", scan_endpoint::INF, "k", scan_endpoint::EXCLUSIVE,
       {B{b0}});
    // (-inf, "k"]  ("k" is less than k1 in L0)
    tc("", scan_endpoint::INF, "k", scan_endpoint::INCLUSIVE,
       {B{b0}});
    // (-inf, "k1234567")  ("k1234567" is less than k1 in L0)
    tc("", scan_endpoint::INF, "k1234567", scan_endpoint::EXCLUSIVE,
       {B{b0}});
    // (-inf, "k1234567"]  ("k1234567" is less than k1 in L0)
    tc("", scan_endpoint::INF, "k1234567", scan_endpoint::INCLUSIVE,
       {B{b0}});
    // (-inf, "k123456781")  ("k123456781" is less than k1 in L11)
    tc("", scan_endpoint::INF, "k123456781", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}});
    // (-inf, "k123456781"]  ("k123456781" is less than k1 in L11)
    tc("", scan_endpoint::INF, "k123456781", scan_endpoint::INCLUSIVE,
       {B{b0, b11}});
    // (-inf, k1)
    tc("", scan_endpoint::INF, k1, scan_endpoint::EXCLUSIVE,
       {B{b0, b11}});
    // (-inf, k1]
    tc("", scan_endpoint::INF, k1, scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{}});
    // (-inf, "k123456789a")  ("k123456789a" is greater than k1 in L11)
    tc("", scan_endpoint::INF, "k123456789a", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11}});
    // (-inf, "k123456789a"]  ("k123456789a" is greater than k1 in L11)
    tc("", scan_endpoint::INF, "k123456789a", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11}});
    // (-inf, "k12x")  ("k12x" is between k1 and k2 in L0)
    tc("", scan_endpoint::INF, "k12x", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k12x"]  ("k12x" is between k1 and k2 in L0)
    tc("", scan_endpoint::INF, "k12x", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k12x0123456")  ("k12x0123456" is between k1 and k2 in L0)
    tc("", scan_endpoint::INF, "k12x0123456", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k12x0123456"]  ("k12x0123456" is between k1 and k2 in L0)
    tc("", scan_endpoint::INF, "k12x0123456", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k2345678")  ("k2345678" is less than k2 in L0)
    tc("", scan_endpoint::INF, "k2345678", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k2345678"]  ("k2345678" is less than k2 in L0)
    tc("", scan_endpoint::INF, "k2345678", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0}});
    // (-inf, "k23456788")  ("k23456788" is less than k2 in L12)
    tc("", scan_endpoint::INF, "k23456788", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}});
    // (-inf, "k23456788"]  ("k23456788" is less than k2 in L12)
    tc("", scan_endpoint::INF, "k23456788", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}});
    // (-inf, k2)
    tc("", scan_endpoint::INF, k2, scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}});
    // (-inf, k2]
    tc("", scan_endpoint::INF, k2, scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{}});
    // (-inf, "k2345678a")  ("k2345678a" is greater than k2 in L12)
    tc("", scan_endpoint::INF, "k2345678a", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12}});
    // (-inf, "k2345678a"]  ("k2345678a" is greater than k2 in L12)
    tc("", scan_endpoint::INF, "k2345678a", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12}});
    // (-inf, "k2345679")  ("k2345679" is greater than k2 in L0)
    tc("", scan_endpoint::INF, "k2345679", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // (-inf, "k2345679"]  ("k2345679" is greater than k2 in L0)
    tc("", scan_endpoint::INF, "k2345679", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // (-inf, "k3")  ("k3" is greater than k2 in L0)
    tc("", scan_endpoint::INF, "k3", scan_endpoint::EXCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // (-inf, "k3"]  ("k3" is greater than k2 in L0)
    tc("", scan_endpoint::INF, "k3", scan_endpoint::INCLUSIVE,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});

    // [k1, k2)
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::EXCLUSIVE,
       {B{}, v1, B{b11, b0, b12}});
    // [k1, k2]
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::INCLUSIVE,
       {B{}, v1, B{b11, b0, b12}, v2, B{}});
    // (k1, k2]
    tc(k1, scan_endpoint::EXCLUSIVE, k2, scan_endpoint::INCLUSIVE,
       {B{b11, b0, b12}, v2, B{}});
    // (k1, k2)
    tc(k1, scan_endpoint::EXCLUSIVE, k2, scan_endpoint::EXCLUSIVE,
       {B{b11, b0, b12}});

    // ["k", +inf)  ("k" is less than k1 in L0)
    tc("k", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // ("k", +inf)  ("k" is less than k1 in L0)
    tc("k", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // ["k1234567", +inf)  ("k1234567" is less than k1 in L0)
    tc("k1234567", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // ("k1234567", +inf)  ("k1234567" is less than k1 in L0)
    tc("k1234567", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // ["k123456781", +inf)  ("k123456781" is less than k1 in L11)
    tc("k123456781", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // ("k123456781", +inf)  ("k123456781" is less than k1 in L11)
    tc("k123456781", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b11}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // [k1, +inf)
    tc(k1, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{}, v1, B{b11, b0, b12}, v2, B{b12, b0}});
    // (k1, +inf)
    tc(k1, scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b11, b0, b12}, v2, B{b12, b0}});
    // ["k123456789a", +inf)  ("k123456789a" is greater than k1 in L11)
    tc("k123456789a", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b11, b0, b12}, v2, B{b12, b0}});
    // ("k123456789a", +inf)  ("k123456789a" is greater than k1 in L11)
    tc("k123456789a", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b11, b0, b12}, v2, B{b12, b0}});
    // ["k12x", +inf)  ("k12x" is between k1 and k2 in L0)
    tc("k12x", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ("k12x", +inf)  ("k12x" is between k1 and k2 in L0)
    tc("k12x", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ["k12x0123456", +inf)  ("k12x0123456" is between k1 and k2 in L0)
    tc("k12x0123456", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ("k12x0123456", +inf)  ("k12x0123456" is between k1 and k2 in L0)
    tc("k12x0123456", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ["k2345678", +inf)  ("k2345678" is less than k2 in L0)
    tc("k2345678", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ("k2345678", +inf)  ("k2345678" is less than k2 in L0)
    tc("k2345678", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0, b12}, v2, B{b12, b0}});
    // ["k23456788", +inf)  ("k23456788" is less than k2 in L12)
    tc("k23456788", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b12}, v2, B{b12, b0}});
    // ("k23456788", +inf)  ("k23456788" is less than k2 in L12)
    tc("k23456788", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b12}, v2, B{b12, b0}});
    // [k2, +inf)
    tc(k2, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{}, v2, B{b12, b0}});
    // (k2, +inf)
    tc(k2, scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b12, b0}});
    // ["k2345678a", +inf)  ("k2345678a" is greater than k2 in L12)
    tc("k2345678a", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b12, b0}});
    // ("k2345678a", +inf)  ("k2345678a" is greater than k2 in L12)
    tc("k2345678a", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b12, b0}});
    // ["k2345679", +inf)  ("k2345679" is greater than k2 in L0)
    tc("k2345679", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF,
       {B{b0}});
    // ("k2345679", +inf)  ("k2345679" is greater than k2 in L0)
    tc("k2345679", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF,
       {B{b0}});

    ASSERT_OK(leave(token));
}

TEST_F(iscan_single_test, l2b1v2) {
    // L0 (b) --- L1 (b) --- L2 (b) k1,k2
    std::string k1("01234567" "abcdefgh" "1");
    std::string k2("01234567" "abcdefgh" "2");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, st, std::string_view(k2), &v2, sizeof(v2)));

    border_node* b0;
    border_node* b1;
    border_node* b2;
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
        ASSERT_EQ(b1->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        b2 = static_cast<border_node*>(b1->get_lv_at(0)->get_next_layer());
    }

    // (-inf, +inf)  full-scan
    tc("", scan_endpoint::INF, "", scan_endpoint::INF,
       {B{b0, b1, b2}, v1, B{b2}, v2, B{b2, b1, b0}});

    // [k1, k2]
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::INCLUSIVE,
       {B{}, v1, B{b2}, v2, B{}});

    // [k1, k2)
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::EXCLUSIVE,
       {B{}, v1, B{b2}});

    // ["", ""] : one-point-range (b0), not-exist
    tc("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE,
       {B{b0}});

    // ["012", "012"] : one-point-range (b0), not-exist
    tc("012", scan_endpoint::INCLUSIVE, "012", scan_endpoint::INCLUSIVE,
       {B{b0}});

    // ["01234567", "01234567"] : one-point-range (b0), not-exist
    tc("01234567", scan_endpoint::INCLUSIVE, "01234567", scan_endpoint::INCLUSIVE,
       {B{b0}});

    // ["01234567a", "01234567a"] : one-point-range (b1), not-exist
    tc("01234567a", scan_endpoint::INCLUSIVE, "01234567a", scan_endpoint::INCLUSIVE,
       {B{b1}});

    // ["01234567abcdefgh", "01234567abcdefgh"] : one-point-range (b1), not-exist
    tc("01234567abcdefgh", scan_endpoint::INCLUSIVE, "01234567abcdefgh", scan_endpoint::INCLUSIVE,
       {B{b1}});

    // [k1, k1] : one-point-range (b2), exist
    tc(k1, scan_endpoint::INCLUSIVE, k1, scan_endpoint::INCLUSIVE,
       {B{}, v1, B{}});

    // ["01234567abcdefghA", "01234567abcdefghA"] : one-point-range (b2), not-exist
    tc("01234567abcdefghA", scan_endpoint::INCLUSIVE, "01234567abcdefghA", scan_endpoint::INCLUSIVE,
       {B{b2}});

    // ["01234567abcdefghABCDEFGHIJ", "01234567abcdefghABCDEFGHIJ"] : one-point-range (b2), not-exist
    tc("01234567abcdefghABCDEFGHIJ", scan_endpoint::INCLUSIVE, "01234567abcdefghABCDEFGHIJ", scan_endpoint::INCLUSIVE,
       {B{b2}});

    ASSERT_OK(leave(token));
}

TEST_F(iscan_single_test, l2b2v2) {
    // L0 (b) --- L1 (b)
    //                +-- L21 (b) k1
    //                +-- L22 (b) k2
    std::string k1("01234567" "abcdefgh" "1");
    std::string k2("01234567" "ijklmnop" "2");
    void* v1 = reinterpret_cast<void*>(uintptr_t(0x0000000081808080));
    void* v2 = reinterpret_cast<void*>(uintptr_t(0x0000000082808080));
    Token token{};
    ASSERT_OK(enter(token));
    ASSERT_OK(put<void*>(token, st, std::string_view(k1), &v1, sizeof(v1)));
    ASSERT_OK(put<void*>(token, st, std::string_view(k2), &v2, sizeof(v2)));

    border_node* b0;
    border_node* b1;
    border_node* b21;
    border_node* b22;
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
        ASSERT_EQ(b1->get_lv_at(0)->get_next_layer()->get_version_border(), true);
        b21 = static_cast<border_node*>(b1->get_lv_at(0)->get_next_layer());
        ASSERT_EQ(b1->get_lv_at(1)->get_next_layer()->get_version_border(), true);
        b22 = static_cast<border_node*>(b1->get_lv_at(1)->get_next_layer());
    }

    // (-inf, +inf)  full-scan
    tc("", scan_endpoint::INF, "", scan_endpoint::INF,
       {B{b0, b1, b21}, v1, B{b21, b1, b22}, v2, B{b22, b1, b0}});

    // [k1, k2]
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::INCLUSIVE,
       {B{}, v1, B{b21, b1, b22}, v2, B{}});

    // [k1, k2)
    tc(k1, scan_endpoint::INCLUSIVE, k2, scan_endpoint::EXCLUSIVE,
       {B{}, v1, B{b21, b1, b22}});

    // (k1, k2]
    tc(k1, scan_endpoint::EXCLUSIVE, k2, scan_endpoint::INCLUSIVE,
       {B{b21, b1, b22}, v2, B{}});

    ASSERT_OK(leave(token));
}

static void iscan_scan_sub(std::map<std::string, void*> entries) {
    Token token{};
    ASSERT_OK(enter(token));

    auto cb = [&](node_version64* nvp, node_version64_body) {
        VLOG(log_debug) << "cb p:" << nvp;
        return false;
    };
    iscan_context* ctx{nullptr};
    ctx_cleaner<iscan_context> cleaner{ctx};
    void* val;
    {
        auto itr = entries.begin();
        ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, false, false, ctx, val, cb))
            << "[" << itr->first << "]";
        EXPECT_EQ(val, (*itr++).second) << "[" << itr->first << "]";
        while (itr != entries.end()) {
            ASSERT_OK(iscan_next(ctx, val, cb)) << "[" << itr->first << "]";
            EXPECT_EQ(val, (*itr++).second) << "[" << itr->first << "]";
        }
        ASSERT_EQ(iscan_next(ctx, val, cb), status::OK_SCAN_END);
        ASSERT_OK(iscan_close(ctx));
    }
    {
        auto itr = entries.rbegin();
        ASSERT_OK(iscan_open(st, "", scan_endpoint::INF, "", scan_endpoint::INF, true, false, ctx, val, cb))
            << "[" << itr->first << "]";
        EXPECT_EQ(val, (*itr++).second) << "[" << itr->first << "]";
        while (itr != entries.rend()) {
            ASSERT_OK(iscan_next(ctx, val, cb)) << "[" << itr->first << "]";
            EXPECT_EQ(val, (*itr++).second) << "[" << itr->first << "]";
        }
        ASSERT_EQ(iscan_next(ctx, val, cb), status::OK_SCAN_END);
        ASSERT_OK(iscan_close(ctx));
    }
}

TEST_F(iscan_single_test, random) {
    std::size_t max_key_len = 20;
    std::size_t entry_size = 500;
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

    std::map<std::string, void*> entries;
    Token token{};
    ASSERT_OK(enter(token));
    for (std::size_t i = 0; i < entry_size; i++) {
        std::stringstream ss{};
        ss << "k";
        auto keylen = engine() % max_key_len;
        while (keylen > 0) {
            auto l = std::min<unsigned int>(sizeof(std::uint32_t) / 4, keylen);
            ss << std::hex << std::setw(l) << std::setfill('0') << (engine() & ((1UL << (4*l)) - 1));
            keylen -= l;
        }
        auto k = ss.str();
        if (entries.find(k) != entries.end()) { i--; continue; }
        void* v = (void*)(uintptr_t)((engine() & 0xffffffffUL) << 8);
        entries.emplace(k, v);
        ASSERT_OK(put<void*>(token, st, k, &v, sizeof(v)));
    }
    EXPECT_NO_FATAL_FAILURE(iscan_scan_sub(entries));
    ASSERT_OK(leave(token));
}

} // namespace yakushima::testing
