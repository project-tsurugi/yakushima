/**
 * @file tree_instance_test.cpp
 */

#include <atomic>

#include "gtest/gtest.h"

#include "border_node.h"
#include "clock.h"
#include "kvs.h"
#include "tree_instance.h"

using namespace yakushima;

namespace yakushima::testing {

class ti : public ::testing::Test {};

TEST_F(ti, basic) { // NOLINT
    tree_instance tin;
    ASSERT_EQ(tin.load_root_ptr(), nullptr);
    border_node a{};
    border_node b{};
    base_node* bn_null{nullptr};
    base_node* bn_a_ptr{&a};
    base_node* bn_b_ptr{&b};
    ASSERT_EQ(true, tin.cas_root_ptr(&bn_null, &bn_a_ptr));
    ASSERT_EQ(tin.load_root_ptr(), bn_a_ptr);
    tin.store_root_ptr(bn_b_ptr);
    ASSERT_EQ(tin.load_root_ptr(), bn_b_ptr);
    std::atomic<std::size_t> meet{0};
    struct S {
        static void work(tree_instance* tin, base_node* own, base_node* against,
                         std::atomic<std::size_t>* meet) {
            meet->fetch_add(1);
            while (meet->load(std::memory_order_acquire) != 2) { _mm_pause(); }
            std::size_t ctr{0};
            for (std::size_t i = 0; i < 100; ++i) {
                base_node* expected = tin->load_root_ptr();
                if (expected != own) {
                    ASSERT_EQ(expected, against);
                } else {
                    ++ctr;
                }
                if (tin->cas_root_ptr(&expected, &own) && expected == own) {
                    sleepMs(1);
                };
            }
            printf("own ctr is %zu\n", ctr); // NOLINT
        }
    };

    std::thread other(S::work, &tin, &a, &b, &meet);
    S::work(&tin, &b, &a, &meet);
    other.join();
}

TEST_F(ti, yakushima_in_yakushima) { // NOLINT
    init();
    std::string storage_name{"1"};
    std::string k{"k"};
    create_storage(storage_name);
    tree_instance tin;
    Token token{};
    while (status::OK != enter(token)) { _mm_pause(); }
    put(token, storage_name, k, &tin);
    std::pair<tree_instance*, std::size_t> ret{};
    ASSERT_EQ(status::OK, get<tree_instance>(storage_name, k, ret));
    leave(token);
    ASSERT_EQ(ret.first->load_root_ptr(), tin.load_root_ptr());
    fin();
}
} // namespace yakushima::testing
