/**
 * @file multi_thread_delete_200_key_test.cpp
 */

#include <algorithm>
#include <array>
#include <mutex>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "glog/logging.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class multi_thread_delete_200_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-delete_multi_"
                                  "thread_delete_200_key_test");
    }

    void SetUp() override {
        init();
        std::call_once(init_, call_once_f);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

std::string test_storage_name{"1"}; // NOLINT

TEST_F(multi_thread_delete_200_key_test, 200_key) { // NOLINT
    /**
      * Concurrent put 200 key.
      * Concurrent remove 200 key.
      */
    constexpr std::size_t ary_size = 200;
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
                    kv.emplace_back(std::string(1, i), std::to_string(i));
                }

                Token token{nullptr};
                while (status::OK != enter(token)) { _mm_pause(); }

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
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }

                ASSERT_EQ(status::OK, leave(token));
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

TEST_F(multi_thread_delete_200_key_test, 200_key_shuffle) { // NOLINT
    /**
      * Concurrent put 200 key.
      * Concurrent remove 200 key.
      * Shuffle data.
      */
    constexpr std::size_t ary_size = 200;
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
                    kv.emplace_back(std::string(1, i), std::to_string(i));
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());
                Token token{};
                ASSERT_EQ(status::OK, enter(token));

                std::shuffle(kv.begin(), kv.end(), engine);
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

                meet->fetch_add(1);
                while (meet->load(std::memory_order_acquire) != max_thread) {
                    _mm_pause();
                }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = remove(token, test_storage_name, k);
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }
                ASSERT_EQ(status::OK, leave(token));
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

} // namespace yakushima::testing