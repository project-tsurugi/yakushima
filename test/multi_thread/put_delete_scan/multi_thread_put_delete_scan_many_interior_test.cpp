/**
 * @file multi_thread_put_delete_scan_test.cpp
 */

#include <algorithm>
#include <array>
#include <mutex>
#include <random>
#include <thread>

#include "kvs.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class multi_thread_put_delete_scan_many_interior_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-multi_thread-put_delete_scan-multi_thread_put_"
                "delete_scan_many_interior_test");
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

TEST_F(multi_thread_put_delete_scan_many_interior_test, // NOLINT
       many_interior) {                                 // NOLINT
    /**
   * concurrent put/delete/scan in the state between none to many split of interior.
   */

    constexpr std::size_t ary_size =
            interior_node::child_length * key_slice_length * 1.4;
    std::size_t th_nm{};
    if (ary_size > std::thread::hardware_concurrency()) {
        th_nm = std::thread::hardware_concurrency();
    } else {
        th_nm = ary_size;
    }

    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);

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
                    if (i <= INT8_MAX) {
                        kv.emplace_back(std::make_pair(std::string(1, i),
                                                       std::to_string(i)));
                    } else {
                        kv.emplace_back(std::make_pair(
                                std::string(i / INT8_MAX, INT8_MAX) +
                                        std::string(1, i % INT8_MAX),
                                std::to_string(i)));
                    }
                }

                Token token{};
                enter(token);
                for (std::size_t j = 0; j < 1; ++j) {
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(token, test_storage_name, k, v.data(),
                                         v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::tuple<std::string, char*, std::size_t>>
                            tuple_list; // NOLINT
                    ASSERT_EQ(status::OK,
                              scan<char>(test_storage_name, "",
                                         scan_endpoint::INF, "",
                                         scan_endpoint::INF, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto&& elem : tuple_list) {
                        if (kv.size() == check_ctr) break;
                        for (auto&& elem2 : kv) {
                            if (std::get<1>(elem2).size() ==
                                        std::get<2>(elem) &&
                                memcmp(std::get<1>(elem2).data(),
                                       std::get<1>(elem),
                                       std::get<2>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }

                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i, th_nm);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                   scan_endpoint::INF, tuple_list);
        ASSERT_EQ(tuple_list.size(), ary_size);
        for (std::size_t j = 0; j < ary_size; ++j) {
            std::string v(std::to_string(j));
            constexpr std::size_t v_index = 1;
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(),
                             v.size()),
                      0);
        }

        destroy();
    }
}

TEST_F(multi_thread_put_delete_scan_many_interior_test, // NOLINT
       many_interior_shuffle) {                         // NOLINT
    /**
   * concurrent put/delete/scan in the state between none to many split of interior with
   * shuffle.
   */

    constexpr std::size_t ary_size =
            interior_node::child_length * key_slice_length * 1.4;
    constexpr std::size_t th_nm{ary_size / 2};

    for (size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id) {
                std::vector<std::pair<std::string, std::string>> kv;
                kv.reserve(ary_size / th_nm);
                // data generation
                for (std::size_t i = (ary_size / th_nm) * th_id;
                     i < (th_id != th_nm - 1 ? (ary_size / th_nm) * (th_id + 1)
                                             : ary_size);
                     ++i) {
                    if (i <= INT8_MAX) {
                        kv.emplace_back(std::make_pair(std::string(1, i),
                                                       std::to_string(i)));
                    } else {
                        kv.emplace_back(std::make_pair(
                                std::string(i / INT8_MAX, INT8_MAX) +
                                        std::string(1, i % INT8_MAX),
                                std::to_string(i)));
                    }
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());
                Token token{};
                enter(token);
                for (std::size_t j = 0; j < 1; ++j) {
                    std::shuffle(kv.begin(), kv.end(), engine);
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = put(token, test_storage_name, k, v.data(),
                                         v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::tuple<std::string, char*, std::size_t>>
                            tuple_list; // NOLINT
                    ASSERT_EQ(status::OK,
                              scan<char>(test_storage_name, "",
                                         scan_endpoint::INF, "",
                                         scan_endpoint::INF, tuple_list));
                    ASSERT_EQ(tuple_list.size() >= kv.size(), true);
                    std::size_t check_ctr{0};
                    for (auto&& elem : tuple_list) {
                        if (kv.size() == check_ctr) { break; }
                        for (auto&& elem2 : kv) {
                            if (std::get<1>(elem2).size() ==
                                        std::get<2>(elem) &&
                                memcmp(std::get<1>(elem2).data(),
                                       std::get<1>(elem),
                                       std::get<2>(elem)) == 0) {
                                ++check_ctr;
                                break;
                            }
                        }
                    }
                    ASSERT_EQ(check_ctr, kv.size());
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        std::string v(std::get<1>(i));
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    std::string v(std::get<1>(i));
                    status ret = put(token, test_storage_name, k, v.data(),
                                     v.size());
                    if (ret != status::OK) {
                        ASSERT_EQ(ret, status::OK);
                        std::abort();
                    }
                }

                leave(token);
            }
        };

        std::vector<std::thread> thv;
        thv.reserve(th_nm);
        for (std::size_t i = 0; i < th_nm; ++i) {
            thv.emplace_back(S::work, i);
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                   scan_endpoint::INF, tuple_list);
        ASSERT_EQ(tuple_list.size(), ary_size);
        for (std::size_t j = 0; j < ary_size; ++j) {
            std::string v(std::to_string(j));
            constexpr std::size_t v_index = 1;
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(),
                             v.size()),
                      0);
        }

        destroy();
    }
}

} // namespace yakushima::testing