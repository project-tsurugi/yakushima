/**
 * @file multi_thread_put_delete_test.cpp
 */

#include <algorithm>
#include <random>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpdt : public ::testing::Test {
    void SetUp() override { init(); }

    void TearDown() override { fin(); }
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(mtpdt, one_interior_many_border_shuffle) { // NOLINT
    /**
   * concurrent put/delete in the state between none to split of interior, which is using
   * shuffled data.
   */

    constexpr std::size_t ary_size =
            interior_node::child_length * key_slice_length / 2;
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
                // data generation
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::make_pair(std::string(1, i),
                                                   std::to_string(i)));
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);

                std::shuffle(kv.begin(), kv.end(), engine);

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(token, test_storage_name, k, v.data(),
                                  v.size()),
                              status::OK);
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(remove(token, test_storage_name, k), status::OK);
                }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(token, test_storage_name, k, v.data(),
                                  v.size()),
                              status::OK);
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

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                   scan_endpoint::INF, tuple_list);
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

TEST_F(mtpdt, second_layer_one_interior_many_border_shuffle) { // NOLINT
    /**
   * concurrent put/delete in the state between none to split of interior, which is using
   * shuffled data.
   */
    constexpr std::size_t ary_size =
            interior_node::child_length * key_slice_length / 2;
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
                // data generation
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::make_pair(std::string(8, INT8_MAX) +
                                                           std::string(1, i),
                                                   std::to_string(i)));
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);

                std::shuffle(kv.begin(), kv.end(), engine);
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(token, test_storage_name, k, v.data(),
                                  v.size()),
                              status::OK);
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(remove(token, test_storage_name, k), status::OK);
                }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(token, test_storage_name, k, v.data(),
                                  v.size()),
                              status::OK);
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

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                   scan_endpoint::INF, tuple_list);
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
