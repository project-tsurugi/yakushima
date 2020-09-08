/**
 * @file multi_thread_delete_test.cpp
 */

#include <algorithm>
#include <array>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class mtdt : public ::testing::Test {
};

TEST_F(mtdt, test1) { // NOLINT
    /**
     * Initial state : multi threads put same null char key slices and different key length to multiple border.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 9;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 5; ++i) {
        kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    for (std::size_t i = 5; i < ary_size; ++i) {
        kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(std::vector<std::tuple<std::string, std::string>> &kv, Token &token) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }
            }

        };

        std::thread t(S::put_work, std::ref(kv2));
        S::put_work(std::ref(kv1));
        t.join();

        t = std::thread(S::remove_work, std::ref(kv2), std::ref(token[0]));
        S::remove_work(std::ref(kv1), std::ref(token[1]));
        t.join();

        ASSERT_EQ(leave(token[0]), status::OK);
        ASSERT_EQ(leave(token[1]), status::OK);
        fin();
    }
}

TEST_F(mtdt, test2) { // NOLINT
    /**
     * Initial state : multi threads put same null char key slices and different key length to multiple border, which
     * is using shuffled data.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 9;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 5; ++i) {
        kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    for (std::size_t i = 5; i < ary_size; ++i) {
        kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv2));
        S::put_work(std::ref(kv1));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv2));
        S::remove_work(std::ref(token.at(1)), std::ref(kv1));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test3) { // NOLINT
    /**
     * Initial state : multi threads put same null char key slices and different key length to single border.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 15;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < 7; ++i) {
        kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    for (std::size_t i = 7; i < ary_size; ++i) {
        kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 200; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv2));
        S::put_work(std::ref(kv1));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test4) { // NOLINT
    /**
     * Initial state : multi threads put same null char key slices and different key length to single border, which
     * is using shuffled data.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 15;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(i, '\0'), std::to_string(i));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test5) { // NOLINT
    /**
     * Initial state : multi threads put until first split of border.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test6) { // NOLINT
    /**
     * Initial state : multi threads put until first split of border, which is using shuffled data.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 100; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test7) { // NOLINT
    /**
     * Initial state : multi threads put between first split of border and first split of interior.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 100;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
        for (size_t h = 0; h < 20; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test8) { // NOLINT
    /**
     * Initial state : multi threads put between first split of border and first split of interior, which is using
     * shuffled data.
     * Concurrent remove against initial state.
     */

    constexpr std::size_t ary_size = 100;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 30; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, k);
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test9) { // NOLINT
    /**
     * Initial state : multi threads put until first split of interior.
     * Concurrent remove against initial state.
     */

    std::size_t ary_size = 241;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 30; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, k);
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

TEST_F(mtdt, test10) { // NOLINT
    /**
     * Initial state : multi threads put until first split of interior, which is using shuffled data.
     * Concurrent remove against initial state.
     */

    std::size_t ary_size = 241;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        kv1.emplace_back(std::string(1, i), std::to_string(i));
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        kv2.emplace_back(std::string(1, i), std::to_string(i));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
        for (std::size_t h = 0; h < 30; ++h) {
#endif
        init();
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void put_work(std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void remove_work(Token &token, std::vector<std::tuple<std::string, std::string>> &kv) {
                for (auto &i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, k);
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(kv1));
        S::put_work(std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        fin();
    }
}

}  // namespace yakushima::testing
