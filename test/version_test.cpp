/**
 * @file version_test.cpp
 */

#include <xmmintrin.h>

#include <future>

#include "gtest/gtest.h"

#include "kvs.h"
#include "version.h"

using namespace yakushima;

namespace yakushima::testing {

class vt : public ::testing::Test {};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(vt, test1) { // NOLINT
    node_version64_body b1{};
    node_version64_body b2{};
    b1.init();
    b2.init();
    ASSERT_EQ(b1, b2);
    b1.set_locked(!b1.get_locked());
    ASSERT_NE(b1, b2);
}

TEST_F(vt, test2) { // NOLINT
    // single update test.
    {
        node_version64 ver;
        auto vinsert_inc_100 = [&ver]() {
            for (auto i = 0; i < 100; ++i) { ver.atomic_inc_vinsert(); }
        };
        vinsert_inc_100();
        ASSERT_EQ(ver.get_body().get_vinsert_delete(), 100);
    }

    // concurrent update test.
    {
        node_version64 ver;
        auto vinsert_inc_100 = [&ver]() {
            for (auto i = 0; i < 100; ++i) { ver.atomic_inc_vinsert(); }
        };
        std::future<void> f = std::async(std::launch::async, vinsert_inc_100);
        vinsert_inc_100();
        f.wait();
        ASSERT_EQ(ver.get_body().get_vinsert_delete(), 200);
    }
}

TEST_F(vt, test3) { // NOLINT
    node_version64_body vb{};
    vb.init();
    ASSERT_EQ(true, true);
    // vb.display();

    node_version64 v{};
    v.init();
    ASSERT_EQ(true, true);
    // v.display();
}

TEST_F(vt, vinsert_delete) { // NOLINT
    init();
    create_storage(test_storage_name);
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    constexpr std::size_t key_length = 2;
    std::array<std::string, key_length> key{};
    std::string v{"v"};
    for (std::size_t i = 0; i < key_length; ++i) {
        key.at(i) = std::string{1, static_cast<char>(i)}; // NOLINT
    }
    Token token{};
    while (status::OK != enter(token)) { _mm_pause(); }
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, key.at(0), v.data(), v.size()));
    std::size_t vid = ti->load_root_ptr()->get_version_vinsert_delete();
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, key.at(1), v.data(), v.size()));
    leave(token);
    ASSERT_EQ(vid + 1, ti->load_root_ptr()->get_version_vinsert_delete());
    fin();
}

TEST_F(vt, vsplit) { // NOLINT
    init();
    create_storage(test_storage_name);
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    constexpr std::size_t key_length = key_slice_length + 1;
    std::array<std::string, key_length> key{};
    std::string v{"v"};
    for (std::size_t i = 0; i < key_length; ++i) {
        key.at(i) = std::string{1, static_cast<char>(i)}; // NOLINT
    }
    Token token{};
    while (status::OK != enter(token)) { _mm_pause(); }
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, key.at(0), v.data(), v.size()));
    std::size_t vid = ti->load_root_ptr()->get_version_vsplit();
    for (std::size_t i = 1; i < key_length; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, key.at(i), v.data(), v.size()));
    }
    leave(token);
    ASSERT_EQ(vid + 1, dynamic_cast<border_node*>(
                               dynamic_cast<interior_node*>(ti->load_root_ptr())
                                       ->get_child_at(0))
                               ->get_version_vsplit());
    fin();
}

} // namespace yakushima::testing
