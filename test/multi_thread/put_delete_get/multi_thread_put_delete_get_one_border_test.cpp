/**
 * @file multi_thread_put_delete_get_one_border_test.cpp
 */

#include <algorithm>
#include <array>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpdgt : public ::testing::Test {
    void SetUp() override { init(); }

    void TearDown() override { fin(); }
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(mtpdgt, one_border_null_key) { // NOLINT
    /**
   * concurrent put/delete/get same null char key slices and different key length to
   * single border by multi threads.
   */
    constexpr std::size_t ary_size = 9;
    std::size_t th_nm{};
    if (ary_size > std::thread::hardware_concurrency()) {
        th_nm = std::thread::hardware_concurrency();
    } else {
        th_nm = ary_size;
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 50; ++h) {
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
                    kv.emplace_back(std::make_pair(std::string(i, '\0'),
                                                   std::to_string(i)));
                }

                Token token{};
                enter(token);

                for (std::size_t j = 0; j < 1; ++j) {
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
                        std::pair<char*, std::size_t> ret{};
                        ASSERT_EQ(status::OK,
                                  get<char>(test_storage_name, k, ret));
                        ASSERT_EQ(memcmp(std::get<0>(ret), v.data(), v.size()),
                                  0);
                    }
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        ASSERT_EQ(remove(token, test_storage_name, k),
                                  status::OK);
                    }
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

TEST_F(mtpdgt, one_border_null_key_shuffle) { // NOLINT
    /**
   * test1 variant which is the test using shuffle order data.
   */

    constexpr std::size_t ary_size = 9;
    std::size_t th_nm{};
    if (ary_size > std::thread::hardware_concurrency()) {
        th_nm = std::thread::hardware_concurrency();
    } else {
        th_nm = ary_size;
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 50; ++h) {
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
                    kv.emplace_back(std::make_pair(std::string(i, '\0'),
                                                   std::to_string(i)));
                }

                std::random_device seed_gen{};
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);

                for (std::size_t j = 0; j < 1; ++j) {
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
                        std::pair<char*, std::size_t> ret{};
                        ASSERT_EQ(status::OK,
                                  get<char>(test_storage_name, k, ret));
                        ASSERT_EQ(memcmp(std::get<0>(ret), v.data(), v.size()),
                                  0);
                    }
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        ASSERT_EQ(remove(token, test_storage_name, k),
                                  status::OK);
                    }
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
