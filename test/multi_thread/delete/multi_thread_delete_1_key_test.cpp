/**
 * @file multi_thread_delete_1_key_test.cpp
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

class multi_thread_delete_1_key_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-delete_multi_"
                                  "thread_delete_1_key_test");
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

TEST_F(multi_thread_delete_1_key_test, 1_key) { // NOLINT
    /**
      * Concurrent put 1 key.
      * Concurrent remove 1 key.
      */
    static constexpr std::size_t th_nm{10};

#ifndef NDEBUG
    for (std::size_t h = 0; h < 1; ++h) {
#else
    for (std::size_t h = 0; h < 10; ++h) {
#endif
        create_storage(st);

        struct S {
            static void work() {
                Token token{nullptr};
                while (status::OK != enter(token)) { _mm_pause(); }

                std::string k{"k"};
                std::string v{"v"};
                for (std::size_t i = 0; i < 100; ++i) {
                    status ret = put(token, st, k, v.data(), v.size());
                    ASSERT_EQ(ret, status::OK);
                    ret = remove(token, st, k);
                }
                ASSERT_EQ(status::OK, leave(token));
            }
        };

        std::vector<std::thread> thv{};
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) { thv.emplace_back(S::work); }
        for (auto&& th : thv) { th.join(); }
        thv.clear();
    }

    destroy();
}

} // namespace yakushima::testing