/**
 * @file multi_thread_put_delete_get_one_interior_many_border_test.cpp
 */

#include <algorithm>
#include <array>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"};// NOLINT

class mtpdgt : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override {
        fin();
    }
};

TEST_F(mtpdgt, many_interior_many_border_shuffle) {// NOLINT
    /**
     * concurrent put/delete/get in the state between none to split of interior, which is using shuffled data.
     */
    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length / 2;
    std::vector<std::pair<std::string, std::string>> kv1;
    std::vector<std::pair<std::string, std::string>> kv2;
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        if (i <= INT8_MAX) {
            kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv1.emplace_back(
                    std::make_pair(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i), std::to_string(i)));
        }
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        if (i <= INT8_MAX) {
            kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv2.emplace_back(
                    std::make_pair(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i), std::to_string(i)));
        }
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 200; ++h) {
#endif
        create_storage(test_storage_name);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token[0]), status::OK);
        ASSERT_EQ(enter(token[1]), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>>& kv, Token& token) {
#ifndef NDEBUG
                for (std::size_t j = 0; j < 1; ++j) {
#else
                for (std::size_t j = 0; j < 10; ++j) {
#endif
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        ASSERT_EQ(put(test_storage_name, k, v.data(), v.size()), status::OK);
                    }
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        std::pair<char*, std::size_t> ret = get<char>(test_storage_name, k);
                        ASSERT_EQ(memcmp(std::get<0>(ret), v.data(), v.size()), 0);
                    }
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        ASSERT_EQ(remove(token, test_storage_name, k), status::OK);
                    }
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(test_storage_name, k, v.data(), v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
        S::work(std::ref(kv1), std::ref(token[1]));
        t.join();

        constexpr std::size_t v_index = 0;
        struct parallel_verify {
            static void work(std::size_t i) {
                std::string k;
                if (i <= INT8_MAX) {
                    k = std::string(1, i);
                } else {
                    k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i);
                }
                std::vector<std::pair<char*, std::size_t>> tuple_list;
                scan<char>(test_storage_name, "", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
                if (tuple_list.size() != i + 1) {
                    scan<char>(test_storage_name, "", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
                    ASSERT_EQ(tuple_list.size(), i + 1);
                }
                for (std::size_t j = 0; j < i + 1; ++j) {
                    std::string v(std::to_string(j));
                    ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
                }
            }
        };
        std::vector<std::thread> th_vc;
        th_vc.reserve(ary_size);
        for (std::size_t i = 0; i < ary_size; ++i) {
            th_vc.emplace_back(parallel_verify::work, i);
        }
        for (auto&& th : th_vc) { th.join(); }

        ASSERT_EQ(leave(token[0]), status::OK);
        ASSERT_EQ(leave(token[1]), status::OK);
        destroy();
    }
}

}// namespace yakushima::testing
