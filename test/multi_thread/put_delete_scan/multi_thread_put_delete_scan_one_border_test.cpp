/**
 * @file multi_thread_put_delete_scan_test.cpp
 */

#include <algorithm>
#include <array>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtpdst : public ::testing::Test {
    void SetUp() override { init(); }
    void TearDown() override { fin(); }
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(mtpdst, one_border) { // NOLINT
    /**
   * concurrent put/delete/scan same null char key slices and different key length to
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
                    for (auto&& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(token, test_storage_name, k, v.data(),
                                         v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::tuple<std::string, char*, std::size_t>>
                            tuple_list; // NOLINT
                    std::string_view left{};
                    std::string_view right{};
                    if (std::get<0>(kv.front()).size() >
                        std::get<0>(kv.back()).size()) {
                        left = std::string_view(std::get<0>(kv.back()));
                        right = std::string_view(std::get<0>(kv.front()));
                    } else {
                        left = std::string_view(std::get<0>(kv.front()));
                        right = std::string_view(std::get<0>(kv.back()));
                    }
                    ASSERT_EQ(status::OK,
                              scan<char>(test_storage_name, left,
                                         scan_endpoint::INCLUSIVE, right,
                                         scan_endpoint::INCLUSIVE, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto&& elem : tuple_list) {
                        if (kv.size() == check_ctr) break;
                        for (auto&& elem2 : kv) {
                            if (memcmp(std::get<1>(elem2).data(),
                                       std::get<1>(elem),
                                       std::get<2>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto&& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }

                for (auto&& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
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

TEST_F(mtpdst, one_border_shuffle) { // NOLINT
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
                    std::shuffle(kv.begin(), kv.end(), engine);
                    for (auto&& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(token, test_storage_name, k, v.data(),
                                         v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::tuple<std::string, char*, std::size_t>>
                            tuple_list; // NOLINT
                    std::string_view left{};
                    std::string_view right{};
                    if (std::get<0>(kv.front()).size() >
                        std::get<0>(kv.back()).size()) {
                        left = std::string_view(std::get<0>(kv.back()));
                        right = std::string_view(std::get<0>(kv.front()));
                    } else {
                        left = std::string_view(std::get<0>(kv.front()));
                        right = std::string_view(std::get<0>(kv.back()));
                    }
                    ASSERT_EQ(status::OK,
                              scan<char>(test_storage_name, left,
                                         scan_endpoint::INCLUSIVE, right,
                                         scan_endpoint::INCLUSIVE, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto&& elem : tuple_list) {
                        if (kv.size() == check_ctr) { break; }
                        for (auto&& elem2 : kv) {
                            if (memcmp(std::get<1>(elem2).data(),
                                       std::get<1>(elem),
                                       std::get<2>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto&& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }

                for (auto&& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
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