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

TEST_F(mtpdst, test7) { // NOLINT
    /**
     * concurrent put/delete/scan in the state between none to split of interior, which is using shuffled data.
     */

    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length / 2;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        if (i <= INT8_MAX) {
            kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv1.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        if (i <= INT8_MAX) {
            kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv2.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }

    std::random_device seed_gen{};
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 200; ++h) {
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

        std::thread t(S::work, std::ref(kv2), std::ref(token[0]));
        S::work(std::ref(kv1), std::ref(token[1]));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k;
            if (i <= INT8_MAX) {
                k = std::string(1, i);
            } else {
                k = std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX);
            }
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
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtpdst, test8) { // NOLINT
    /**
     * concurrent put/delete/scan in the state between none to many split of interior.
     */

    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        if (i <= INT8_MAX) {
            kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv1.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        if (i <= INT8_MAX) {
            kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv2.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }

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

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

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
            std::string k{};
            if (i <= INT8_MAX) {
                k = std::string(1, i);
            } else {
                k = std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX);
            }
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
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtpdst, test9) { // NOLINT
    /**
     * concurrent put/delete/scan in the state between none to many split of interior with shuffle.
     */

    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 1.4;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        if (i <= INT8_MAX) {
            kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv1.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        if (i <= INT8_MAX) {
            kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv2.emplace_back(
                    std::make_pair(
                            std::string(i / INT8_MAX, static_cast<char>(INT8_MAX)) + std::string(1, i - INT8_MAX),
                            std::to_string(i)));
        }
    }

    std::random_device seed_gen{};
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
        for (size_t h = 0; h < 10; ++h) {
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
            std::string k{};
            if (i <= INT8_MAX) {
                k = std::string(1, i);
            } else {
                k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX);
            }
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
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtpdst, test10) { // NOLINT
    /**
     * multi-layer put-delete-scan test.
     */
    constexpr std::size_t ary_size = interior_node::child_length * base_node::key_slice_length * 10;
    std::vector<std::pair<std::string, std::string>> kv1; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        if (i <= INT8_MAX) {
            kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv1.emplace_back(
                    std::make_pair(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX),
                                   std::to_string(i)));
        }
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        if (i <= INT8_MAX) {
            kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
        } else {
            kv2.emplace_back(
                    std::make_pair(std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX),
                                   std::to_string(i)));
        }
    }

    std::random_device seed_gen{};
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
        for (size_t h = 0; h < 10; ++h) {
#endif
        init();
        create_storage(test_storage_name);
        std::atomic<base_node*>* target_storage{};
        find_storage(test_storage_name, &target_storage);
        ASSERT_EQ(target_storage->load(std::memory_order_acquire), nullptr);
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
                        if (status::OK != ret) {
                            ASSERT_EQ(status::OK, ret);
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
                        if (status::OK != ret) {
                            ASSERT_EQ(status::OK, ret);
                            std::abort();
                        }
                    }
                }
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(test_storage_name, k, v.data(), v.size());
                    if (status::OK != ret) {
                        ASSERT_EQ(status::OK, ret);
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
        for (std::size_t i = 0; i < ary_size / 100; ++i) {
            std::string k;
            if (i <= INT8_MAX) {
                k = std::string(1, i);
            } else {
                k = std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i - INT8_MAX);
            }
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
        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

}  // namespace yakushima::testing
