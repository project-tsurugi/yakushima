/**
 * @file multi_thread_put_1K_key_test.cpp
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

class multi_thread_put_100k_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-put-multi_"
                                  "thread_put_100k_key_test");
        FLAGS_stderrthreshold = 0;
    }

    void SetUp() override {
        init();
        std::call_once(init_, call_once_f);
    }

    void TearDown() override { fin(); }

private:
    static inline std::once_flag init_; // NOLINT
};

std::string st{"1"}; // NOLINT

#ifndef NDEBUG
TEST_F(multi_thread_put_100k_key_test, DISABLED_100k_key) { // NOLINT
#else
TEST_F(multi_thread_put_100k_key_test, 100k_key) {         // NOLINT
#endif
    /**
      * Concurrent put 100k key.
      */
    constexpr std::size_t ary_size = 100000;
    std::size_t th_nm{std::thread::hardware_concurrency()};

    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(st);

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
                    std::string k{"12345678"};
                    memcpy(k.data(), &i, sizeof(i));
                    kv.emplace_back(k, "v");
                }

                Token token{nullptr};
                while (status::OK != enter(token)) { _mm_pause(); }

                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, st, k, v.data(), v.size());
                    if (ret != status::OK) {
                        LOG(FATAL) << ret; // output log
                        std::abort();
                    }
                }

                ASSERT_EQ(status::OK, leave(token));
            }
        };

        std::vector<std::thread> thv{};
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

#ifndef NDEBUG
TEST_F(multi_thread_put_100k_key_test, DISABLED_100k_key_shuffle) { // NOLINT
#else
TEST_F(multi_thread_put_100k_key_test, 100k_key_shuffle) { // NOLINT
#endif
    /**
      * Concurrent put 100k key.
      * Shuffle data.
      */
    constexpr std::size_t ary_size = 1000000;
    std::size_t th_nm{std::thread::hardware_concurrency()};

    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(st);

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
                    std::string k{"12345678"};
                    memcpy(k.data(), &i, sizeof(i));
                    kv.emplace_back(k, "v");
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());
                Token token{};
                ASSERT_EQ(status::OK, enter(token));

                std::shuffle(kv.begin(), kv.end(), engine);
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, st, k, v.data(), v.size());
                    if (ret != status::OK) {
                        EXPECT_EQ(status::OK, ret); // output log
                        std::abort();
                    }
                }

                ASSERT_EQ(status::OK, leave(token));
            }
        };

        std::vector<std::thread> thv{};
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

} // namespace yakushima::testing