/**
 * @file multi_thread_delete_test.cpp
 */

#include <algorithm>
#include <array>
#include <mutex>
#include <random>
#include <thread>

#include "kvs.h"

#include "gtest/gtest.h"

#include "glog/logging.h"
using namespace yakushima;

namespace yakushima::testing {

class multi_thread_delete_10_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-delete-multi_"
                                  "thread_delete_10_key_test");
        FLAGS_stderrthreshold = 0;
    }
    void SetUp() override {
        std::call_once(init_, call_once_f);
        init();
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

std::string st{"1"}; // NOLINT

TEST_F(multi_thread_delete_10_key_test, ordered_10_key) { // NOLINT
    /**
      * Concurrent remove against 10 key.
      */

    constexpr std::size_t ary_size = 10;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    std::string k{8, 0};
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv1.emplace_back(k, "v");
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv2.emplace_back(k, "v");
    }

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
    for (size_t h = 0; h < 20; ++h) {
#endif
        create_storage(st);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void
            put_work(Token& token,
                     std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, st, k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void
            remove_work(Token& token,
                        std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, st, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(token.at(0)), std::ref(kv1));
        S::put_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        destroy();
    }
}

TEST_F(multi_thread_delete_10_key_test, reverse_10_key) { // NOLINT
    /**
      * Concurrent remove against 100 key.
      */

    constexpr std::size_t ary_size = 10;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    std::string k{8, 0};
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv1.emplace_back(k, "v");
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv2.emplace_back(k, "v");
    }

#ifndef NDEBUG
    for (size_t h = 0; h < 1; ++h) {
#else
    for (size_t h = 0; h < 20; ++h) {
#endif
        create_storage(st);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::reverse(kv1.begin(), kv1.end());
        std::reverse(kv2.begin(), kv2.end());

        struct S {
            static void
            put_work(Token& token,
                     std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, st, k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void
            remove_work(Token& token,
                        std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, st, std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(token.at(0)), std::ref(kv1));
        S::put_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        destroy();
    }
}

TEST_F(multi_thread_delete_10_key_test, shuffled_10_key) { // NOLINT
    /**
      * Concurrent remove against 100 key.
      */
    constexpr std::size_t ary_size = 10;
    std::vector<std::tuple<std::string, std::string>> kv1{}; // NOLINT
    std::vector<std::tuple<std::string, std::string>> kv2{}; // NOLINT
    std::string k{8, 0};
    for (std::size_t i = 0; i < ary_size / 2; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv1.emplace_back(k, "v");
    }
    for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
        memcpy(k.data(), &i, sizeof(i));
        kv2.emplace_back(k, "v");
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 30; ++h) {
#endif
        create_storage(st);
        std::array<Token, 2> token{};
        ASSERT_EQ(enter(token.at(0)), status::OK);
        ASSERT_EQ(enter(token.at(1)), status::OK);

        std::shuffle(kv1.begin(), kv1.end(), engine);
        std::shuffle(kv2.begin(), kv2.end(), engine);

        struct S {
            static void
            put_work(Token& token,
                     std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, st, k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }

            static void
            remove_work(Token& token,
                        std::vector<std::tuple<std::string, std::string>>& kv) {
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, st, k);
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(token.at(0)), std::ref(kv1));
        S::put_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        destroy();
    }
}

} // namespace yakushima::testing