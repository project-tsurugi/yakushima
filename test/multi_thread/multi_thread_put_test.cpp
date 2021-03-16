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
};

TEST_F(mtpt, concurrent_put_against_single_border) { // NOLINT
    constexpr std::size_t ary_size = 9;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 5; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = 5; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_against_single_border_with_shuffle) { // NOLINT

    constexpr std::size_t ary_size = 9;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 5; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = 5; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        destroy();
        fin();
    }
}

TEST_F(mtpt, concurrent_put_against_multiple_border_multiple_layer) { // NOLINT

    constexpr std::size_t ary_size = 15;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 7; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = 7; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_against_multiple_border_multiple_layer_with_shuffle) { // NOLINT

    constexpr std::size_t ary_size = 15;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 7; ++i) {
        kv1.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    for (std::size_t i = 7; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(i, '\0'), std::to_string(i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 30; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(i, '\0');
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_untill_first_split_border) { // NOLINT

    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, 'a' + i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, 'a' + i), std::to_string(i)));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, 'a' + i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_untill_first_split_border_with_shuffle) { // NOLINT

    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, 'a' + i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, 'a' + i), std::to_string(i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, 'a' + i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_many_split_border) { // NOLINT

    constexpr std::size_t ary_size = 100;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
        for (size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            if (tuple_list.size() != i + 1) {
                EXPECT_EQ(tuple_list.size(), i + 1);
            }
            ASSERT_EQ(tuple_list.size(), i + 1);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_many_split_border_with_shuffle) { // NOLINT
    constexpr std::size_t ary_size{100};
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            if (tuple_list.size() != i + 1) {
                EXPECT_EQ(tuple_list.size(), i + 1);
            }
            ASSERT_EQ(tuple_list.size(), i + 1);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_untill_first_split_interior) { // NOLINT
    constexpr std::size_t ary_size{241}; // value fanout 15 * node fanout 16 + 1
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(std::string_view(k), v.data(), v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            ASSERT_EQ(tuple_list.size(), i + 1);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);
        fin();
    }
}

TEST_F(mtpt, concurrent_put_untill_first_split_interior_with_shuffle) { // NOLINT

    std::size_t ary_size = 241;
    std::vector<std::pair<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::pair<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::make_pair(std::string(1, i), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        Token token{};
        ASSERT_EQ(enter(token), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void work(std::vector<std::pair<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    ASSERT_EQ(put(std::string_view(k), v.data(), v.size()), status::OK);
                }
            }
        };

        std::thread t(S::work, std::ref(kv2));
        S::work(std::ref(kv1));
        t.join();

        std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
        constexpr std::size_t v_index = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            std::string k(1, i);
            scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tuple_list);
            if (tuple_list.size() != i + 1) {
                ASSERT_EQ(tuple_list.size(), i + 1);
            }

            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
            }
        }
        ASSERT_EQ(leave(token), status::OK);

        fin();
    }
}

}  // namespace yakushima::testing
