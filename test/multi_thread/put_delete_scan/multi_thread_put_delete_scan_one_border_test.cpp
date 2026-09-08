/**
 * @file multi_thread_put_delete_scan_test.cpp
 */

#include <algorithm>
#include <array>
#include <random>
#include <thread>

#include "test_tool.h"

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
                while (enter(token) != status::OK) { _mm_pause(); }

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
                        left = std::get<0>(kv.back());
                        right = std::get<0>(kv.front());
                    } else {
                        left = std::get<0>(kv.front());
                        right = std::get<0>(kv.back());
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
                while (enter(token) != status::OK) { _mm_pause(); }

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
                        left = std::get<0>(kv.back());
                        right = std::get<0>(kv.front());
                    } else {
                        left = std::get<0>(kv.front());
                        right = std::get<0>(kv.back());
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

TEST_F(mtpdst, never_read_null_lv) {
    // check: delete_at for varlen (not-inlined) value
    // regardless of concurrent schedule, remove() must not result in scan() reading NULL value
    // calling init_lv() in delete_at() may cause the problem
    constexpr std::size_t ary_size = 12; // <= key_slice_length

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 50; ++h) {
#endif
        create_storage(test_storage_name);
        static std::atomic_bool end_flag = false;
        struct S {
            static std::string make_key(std::size_t k) {
                return std::to_string(k);
            }
            static std::string make_value(std::size_t k) {
                return std::to_string(k);
            }
            static void modify_work(std::size_t th_id) {
                Token token{};
                for (std::size_t i = 0; i < 100; i++) {
                    while (enter(token) != status::OK) { _mm_pause(); }
                    std::string v = make_value(th_id);
                    VLOG(41) << "put    th_id:" << th_id << " v:" << v;
                    ASSERT_OK(put(token, test_storage_name, make_key(th_id), v.data(), v.size()));
                    _mm_pause();
                    VLOG(41) << "remove th_id:" << th_id << " v:" << v;
                    ASSERT_OK(remove(token, test_storage_name, make_key(th_id)));
                    _mm_pause();
                    leave(token);
                }
            }
            static void scan_work(std::size_t th_id) {
                while (!end_flag) {
                    Token token{};
                    while (enter(token) != status::OK) { _mm_pause(); }
                    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list{};
                    ASSERT_OK(scan<char>(test_storage_name, "", scan_endpoint::INF, "", scan_endpoint::INF,
                                         tuple_list));
                    for (auto&& elem : tuple_list) {
                        auto k = std::get<0>(elem);
                        if (std::get<1>(elem) == nullptr) {
                            // using VLOG to display timestamp in the same format as modify_work()
                            VLOG(41) << "scan returns null value";
                            EXPECT_NE(std::get<1>(elem), nullptr) << "th_id:" << th_id << " k:" << k;
                            continue;
                        }
                        std::string_view scanned{std::get<1>(elem), std::get<2>(elem)};
                        ASSERT_EQ(k, scanned) << "th_id:" << th_id;
                    }
                    leave(token);
                }
            }
        };

        std::vector<std::thread> mth{};
        std::vector<std::thread> sth{};
        mth.reserve(ary_size);
        sth.reserve(5);
        for (std::size_t i = 0; i < ary_size; ++i) { mth.emplace_back(S::modify_work, i); }
        for (std::size_t i = 0; i < 5; ++i) { sth.emplace_back(S::scan_work, i); }
        for (auto&& th : mth) { th.join(); }
        end_flag = true;
        for (auto&& th : sth) { th.join(); }
        mth.clear();
        sth.clear();

        destroy();
    }
}

} // namespace yakushima::testing
