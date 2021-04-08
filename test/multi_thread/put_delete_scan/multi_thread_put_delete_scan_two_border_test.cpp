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
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(mtpdst, test3) { // NOLINT
    /**
     * multiple put/delete/scan same null char key whose length is different each other against multiple border,
     * which is across some layer.
     */

    constexpr std::size_t ary_size = 15;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }

    std::random_device seed_gen{};
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 10; ++h) {
#endif
        init();
        create_storage(test_storage_name);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);
        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv, Token &token) {
                for (std::size_t j = 0; j < 10; ++j) {
                    for (auto &i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(test_storage_name, k, v.data(), v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::pair<char*, std::size_t>> tuple_list; // NOLINT
                    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto &&elem : tuple_list) {
                        if (kv.size() == check_ctr) break;
                        for (auto &&elem2 : kv) {
                            if (std::get<1>(elem2).size() == std::get<1>(elem) &&
                                memcmp(std::get<1>(elem2).data(), std::get<0>(elem), std::get<1>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto &i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }
                for (auto &i : kv) {
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

        std::thread t(S::work, std::ref(kv2), std::ref(token.at(0)));
        S::work(std::ref(kv1), std::ref(token.at(1)));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>(test_storage_name, "", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtpdst, test4) { // NOLINT
    /**
     * test3 variant which is the test using shuffle order data.
     */

    constexpr std::size_t ary_size = 15;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < 5; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = 5; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }

    std::random_device seed_gen{};
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 10; ++h) {
#endif
        init();
        create_storage(test_storage_name);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv, Token &token) {
                for (std::size_t j = 0; j < 10; ++j) {
                    for (auto &i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(test_storage_name, k, v.data(), v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::pair<char*, std::size_t>> tuple_list; // NOLINT
                    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF, "", scan_endpoint::INF, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto &&elem : tuple_list) {
                        if (kv.size() == check_ctr) break;
                        for (auto &&elem2 : kv) {
                            if (std::get<1>(elem2).size() == std::get<1>(elem) &&
                                memcmp(std::get<1>(elem2).data(), std::get<0>(elem), std::get<1>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto &i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }
                for (auto &i : kv) {
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

        std::thread t(S::work, std::ref(kv2), std::ref(token.at(0)));
        S::work(std::ref(kv1), std::ref(token.at(1)));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>(test_storage_name, "", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

}