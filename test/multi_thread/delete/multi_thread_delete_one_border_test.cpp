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

class multi_thread_delete_one_border_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-delete-multi_"
                                  "thread_delete_one_border_test");
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

std::string test_storage_name{"1"}; // NOLINT

TEST_F(multi_thread_delete_one_border_test, one_border) { // NOLINT
    /**
      * Initial state : multi threads put same null char key slices and 
      * different key length to multiple border. Concurrent remove against 
      * initial state.
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
    for (std::size_t h = 0; h < 100; ++h) {
#endif
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id, std::size_t max_thread,
                             std::atomic<std::size_t>* meet) {
                std::vector<std::pair<std::string, std::string>> kv;
                kv.reserve(ary_size / max_thread);
                // data generation
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::string(i, '\0'), std::to_string(i));
                }

                Token token{};
                enter(token);

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }

                meet->fetch_add(1);
                while (meet->load(std::memory_order_acquire) != max_thread) {
                    _mm_pause();
                }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, test_storage_name,
                                        std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }
                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        std::atomic<std::size_t> meet{0};
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm, &meet);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

TEST_F(multi_thread_delete_one_border_test, one_border_shuffle) { // NOLINT
    /**
      * Initial state : multi threads put same null char key slices and 
      * different key length to multiple border, which is using shuffled data. 
      * Concurrent remove against initial state.
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
    for (std::size_t h = 0; h < 100; ++h) {
#endif
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id, std::size_t max_thread,
                             std::atomic<std::size_t>* meet) {
                std::vector<std::pair<std::string, std::string>> kv;
                kv.reserve(ary_size / max_thread);
                // data generation
                for (std::size_t i = (ary_size / max_thread) * th_id;
                     i < (th_id != max_thread - 1
                                  ? (ary_size / max_thread) * (th_id + 1)
                                  : ary_size);
                     ++i) {
                    kv.emplace_back(std::string(i, '\0'), std::to_string(i));
                }

                std::random_device seed_gen{};
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);

                std::shuffle(kv.begin(), kv.end(), engine);

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }

                meet->fetch_add(1);
                while (meet->load(std::memory_order_acquire) != max_thread) {
                    _mm_pause();
                }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, test_storage_name,
                                        std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(ret, status::OK); // output log
                        std::abort();
                    }
                }
                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        std::atomic<std::size_t> meet{0};
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm, &meet);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

TEST_F(multi_thread_delete_one_border_test, test3) { // NOLINT
    /**
      * Initial state : multi threads put same null char key slices and 
      * different key length to single border. Concurrent remove against 
      * initial state.
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
        create_storage(test_storage_name);
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
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
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
                    status ret = remove(token, test_storage_name,
                                        std::string_view(k));
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
            }
        };

        std::thread t(S::put_work, std::ref(token.at(0)), std::ref(kv2));
        S::put_work(std::ref(token.at(1)), std::ref(kv1));
        t.join();

        t = std::thread(S::remove_work, std::ref(token.at(0)), std::ref(kv1));
        S::remove_work(std::ref(token.at(1)), std::ref(kv2));
        t.join();

        ASSERT_EQ(leave(token.at(0)), status::OK);
        ASSERT_EQ(leave(token.at(1)), status::OK);
        destroy();
    }
}

TEST_F(multi_thread_delete_one_border_test, test4) { // NOLINT
    /**
      * Initial state : multi threads put same null char key slices and 
      * different key length to single border, which is using shuffled data. 
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
        create_storage(test_storage_name);
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
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
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
                    status ret = remove(token, test_storage_name,
                                        std::string_view(k));
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