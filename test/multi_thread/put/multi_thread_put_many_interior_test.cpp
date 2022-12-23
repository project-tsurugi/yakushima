/**
 * @file multi_thread_put_test.cpp
 */

#include <algorithm>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpt : public ::testing::Test {
    void SetUp() override { init(); }

    void TearDown() override { fin(); }
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(mtpt, many_interior) {            // NOLINT
    constexpr std::size_t ary_size{241}; // value fanout 15 * node fanout 16 + 1
    std::size_t th_nm{};
    if (ary_size > std::thread::hardware_concurrency()) {
        th_nm = std::thread::hardware_concurrency();
    } else {
        th_nm = ary_size;
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 10; ++h) {
#endif
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id, std::size_t max_thread) {
                std::vector<std::pair<std::string, std::string>> kv;
                kv.reserve(ary_size / max_thread);
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::make_pair(std::string(1, i),
                                                   std::to_string(i)));
                }

                Token token{};
                enter(token);
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name,
                                     std::string_view(k), v.data(), v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }
                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        std::string k(1, ary_size - 1);
        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list{}; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                   scan_endpoint::INCLUSIVE, tuple_list);
        ASSERT_EQ(tuple_list.size(), ary_size);

        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string v(std::to_string(i));
            constexpr std::size_t v_index = 1;
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(i)), v.data(),
                             v.size()),
                      0);
        }

        destroy();
    }
}

TEST_F(mtpt, many_interior_shuffle) {    // NOLINT
    constexpr std::size_t ary_size{241}; // value fanout 15 * node fanout 16 + 1
    std::size_t th_nm{};
    if (ary_size > std::thread::hardware_concurrency()) {
        th_nm = std::thread::hardware_concurrency();
    } else {
        th_nm = ary_size;
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 10; ++h) {
#endif
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id, std::size_t max_thread) {
                std::vector<std::pair<std::string, std::string>> kv;
                kv.reserve(ary_size / max_thread);
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::make_pair(std::string(1, i),
                                                   std::to_string(i)));
                }

                std::random_device seed_gen{};
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);
                std::shuffle(kv.begin(), kv.end(), engine);
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name,
                                     std::string_view(k), v.data(), v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }
                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        std::string k(1, ary_size - 1);
        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list{}; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                   scan_endpoint::INCLUSIVE, tuple_list);
        ASSERT_EQ(tuple_list.size(), ary_size);
        for (std::size_t j = 0; j < ary_size; ++j) {
            std::string v(std::to_string(j));
            constexpr std::size_t v_index = 1;
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(),
                             v.size()),
                      0);
        }

        destroy();
    }
}

} // namespace yakushima::testing
