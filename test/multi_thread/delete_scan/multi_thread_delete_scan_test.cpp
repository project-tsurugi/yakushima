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

// NOLINTNEXTLINE(*-macro-usage)
#define ASSERT_OK(x) ASSERT_EQ(x, status::OK)

namespace yakushima::testing {

class multi_thread_delete_scan_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging("yakushima-test-multi_thread-delete_scan-multi_thread_delete_scan_test");
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

std::string test_storage_name{"1"}; // NOLINT
constexpr static std::size_t remaining_size = key_slice_length / 2 + 1;

auto make_key(std::size_t i) {
    std::ostringstream ss;
    ss << std::hex << std::setw(2) << std::setfill('0') << i;
    return ss.str();
}

TEST_F(multi_thread_delete_scan_test, delete_border_leaf_during_scan) { // NOLINT
    // insert 0, 1, ..., 119 (8*15 nodes) sequentially
    // -> made 0-7, 8-15, 16-23, ..., 112-119
    // scan thread(1-many) : full scan
    // delete thread(15) for each border : delete 7 nodes
    // check: scan result is not broken: ordered, no dup

    std::size_t num_del_th{std::min<std::size_t>(key_slice_length, std::thread::hardware_concurrency())};
    std::size_t num_scan_th = 5;

    for (std::size_t r = 0; r < 1; ++r) {
        create_storage(test_storage_name);
        {
            // setup initial tree
            Token token{};
            while (enter(token) != status::OK) { _mm_pause(); }
            for (std::size_t i = 0; i < remaining_size * key_slice_length; i++) {
                auto k = make_key(i);
                ASSERT_OK(put<char>(token, test_storage_name, k, k.data(), k.size()));
            }
            leave(token);
        }

        struct S {
            static void work(std::size_t th_id) {
                Token token{};
                while (enter(token) != status::OK) { _mm_pause(); }

                for (std::size_t j = 0; j < 5; ++j) {
                    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list; // NOLINT
                    ASSERT_OK(scan<char>(test_storage_name, {}, scan_endpoint::INF, {}, scan_endpoint::INF, tuple_list));
                    std::string check_key = std::get<0>(*tuple_list.begin());
                    for (auto itr = tuple_list.begin() + 1;
                         itr != tuple_list.end(); ++itr) { // NOLINT
                        if (check_key >= std::get<0>(*itr)) {
                            LOG(INFO) << "it found duplicate. thread " << th_id;
                            std::ostringstream ss{};
                            std::string check_key2 = std::get<0>(*tuple_list.begin());
                            ss << "keys[ " << check_key2;
                            for (auto itr_2 = tuple_list.begin() + 1; // NOLINT
                                 itr_2 != tuple_list.end(); ++itr_2) {
                                ss << " " << std::get<0>(*itr_2);
                                if (check_key2 >= std::get<0>(*itr_2)) { ss << "!!"; }
                                check_key2 = std::get<0>(*itr_2);
                            }
                            LOG(INFO) << ss.str() << " ]";
                            ASSERT_LT(check_key, std::get<0>(*itr));
                            LOG(FATAL);
                        }
                        check_key = std::get<0>(*itr);
                    }
                }

                leave(token);
            }
        };

        struct D {
            static void work(std::size_t th_id) {
                Token token{};
                while (enter(token) != status::OK) { _mm_pause(); }

                auto k = make_key(th_id * remaining_size + 1);
                ASSERT_OK(remove(token, test_storage_name, k));
                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(num_del_th + num_scan_th);
        for (std::size_t i_d = 0, i_s = 0; i_d + i_s < num_del_th + num_scan_th; ) {
            if (i_s < num_scan_th) { thv.emplace_back(S::work, i_s++); }
            if (i_d < num_del_th)  { thv.emplace_back(D::work, i_d++); }
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

} // namespace yakushima::testing
