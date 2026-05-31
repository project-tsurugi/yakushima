/**
 * @file
 * @brief multi thread tests that perform put, delete and iscan
 * @remark derived from multi_thread_put_delete_scan_many_interior_test (scan version)
 */

#include <algorithm>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>

#include "kvs.h"

#include "glog/logging.h"
#include "gtest/gtest.h"

using namespace yakushima;

namespace yakushima::testing {

class multi_thread_put_delete_iscan_many_interior_test : public ::testing::Test {
public:
    static void call_once_f() {
        google::InitGoogleLogging(
                "yakushima-test-multi_thread-put_delete_iscan-multi_thread_put_"
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
std::mutex debug_mtx;               // NOLINT

static std::string hexstr(const std::string& str) {
    std::ostringstream ss{};
    ss << std::hex;
    bool first{true};
    for (auto c : str) {
        if (first) { first = false; }
        else { ss << '-'; }
        ss << std::setw(2) << std::setfill('0')
           << static_cast<unsigned int>(static_cast<unsigned char>(c));
    }
    return ss.str();
}

static std::string value_varchar(std::size_t i) {
    return std::to_string(i);
}

static bool eq_varchar(char* p, const std::string& v) {
    return memcmp(p, v.data(), v.size()) == 0;
}

class pointer {
public:
    explicit pointer(char* p) : p_(p) {}
    std::size_t size() { return sizeof(p_); }
    char** data() { return &p_; }
    bool operator==(pointer o) { return p_ == o.p_; }
private:
    char* p_;
};

constexpr std::size_t ary_size = interior_node::child_length * key_slice_length * 1.4;

static pointer value_inlined(std::size_t i) {
    static std::string pointers(ary_size + 8, 'x'); // prepare many different pointer; +8 for display()
    return pointer{&pointers[i]};
}

static bool eq_inlined(char* p, const pointer& v) {
    return pointer(p) == v;
}


template<class valtype, valtype(*value)(std::size_t), bool(*eq)(char*, const valtype&),
         bool right_to_left, bool do_shuffle = false, std::size_t repeats = 1U>
void many_interior_comm() {
    std::size_t th_nm{std::min<std::size_t>(ary_size, std::thread::hardware_concurrency())};
    LOG(INFO) << "process " << ary_size << " nodes by " << th_nm << " threads";
    static auto make_key = [](std::size_t i) {
        if (i <= INT8_MAX) {
            return std::string(1, i);
        } else {
            return std::string(i / INT8_MAX, INT8_MAX) + std::string(1, i % INT8_MAX);
        }
    };

    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);

        struct S {
            static void work(std::size_t th_id, std::size_t max_thread) {
                std::vector<std::pair<std::string, valtype>> kv;
                kv.reserve(ary_size / max_thread);
                // data generation
                std::size_t kmin = (ary_size / max_thread) * th_id;
                std::size_t kmax = th_id != max_thread - 1
                                   ? (ary_size / max_thread) * (th_id + 1)
                                   : ary_size;
                for (std::size_t i = kmin; i < kmax; ++i) {
                    kv.emplace_back(make_key(i), value(i));
                }

                std::random_device seed_gen;
                std::mt19937 engine(seed_gen());

                Token token{};
                while (enter(token) != status::OK) { _mm_pause(); }

                for (std::size_t j = 0; j < repeats; ++j) {
                    if (do_shuffle) {
                        std::shuffle(kv.begin(), kv.end(), engine);
                    }
                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        valtype v(std::get<1>(i));
                        VLOG(40) << "insert1 " << hexstr(k);
                        status ret = put(token, test_storage_name, k, v.data(), v.size());
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                    std::vector<std::pair<std::string, char*>> res_kv{};
                    void* val;
                    iscan_context* ctx;
                    auto rc = iscan_open(test_storage_name, "", scan_endpoint::INF, "", scan_endpoint::INF,
                                         right_to_left, false, ctx, val);
                    while (true) {
                        if (rc == status::OK_SCAN_END) { break; }
                        if (rc != status::OK) {
                            LOG(ERROR);
                        }
                        res_kv.emplace_back(ctx->full_key(), (char*)val);
                        rc = iscan_next(ctx, val);
                    }
                    if (ctx) { iscan_close(ctx); }
                    if (rc != status::OK_SCAN_END) { LOG(FATAL); }

                    // this thread put kv entries, so at lest, it must find it.
                    std::size_t check_ctr{0};
                    std::stringstream ss_chkv{};
                    for (auto&& elem : res_kv) {
                        ss_chkv << hexstr(elem.first);
                        for (auto&& elem2 : kv) {
                            if (eq(std::get<1>(elem), std::get<1>(elem2))) {
                                ++check_ctr;
                                ss_chkv << "*";
                                break;
                            }
                        }
                        ss_chkv << " ";
                        if (kv.size() == check_ctr) { break; }
                    }
                    if (check_ctr != kv.size()) {
                        // to show glog thread id
                        LOG(INFO) << "[ " << ss_chkv.str() << "] kv:[" << hexstr(make_key(kmin)) << ":" << hexstr(make_key(kmax)) << "]";
                        ASSERT_EQ(check_ctr, kv.size());
                    }
                    // check success for own puts. check duplicate about key.
                    std::string check_key = std::get<0>(*res_kv.begin());
                    for (auto itr = res_kv.begin() + 1;
                         itr != res_kv.end(); ++itr) { // NOLINT
                        if (check_key.compare(std::get<0>(*itr)) * (right_to_left ? -1 : 1) >= 0) {
                            std::unique_lock lk{debug_mtx}; // why need this???
                            LOG(INFO) << "it found duplicate. thread " << th_id;
                            std::ostringstream ss{};
                            std::string check_key2 = std::get<0>(*res_kv.begin());
                            ss << "keys[ " << hexstr(check_key2);
                            for (auto itr_2 = res_kv.begin() + 1; // NOLINT
                                 itr_2 != res_kv.end(); ++itr_2) {
                                ss << " " << hexstr(std::get<0>(*itr_2));
                                if (check_key2 >= std::get<0>(*itr_2)) { ss << "!!"; }
                                check_key2 = std::get<0>(*itr_2);
                            }
                            LOG(INFO) << ss.str() << " ]";
                            display(); // it's not thread-safe, but something is already broken
                            ASSERT_LT(check_key, std::get<0>(*itr)) << "KNOWN-ISSUE: tcn#8 (ti#1299 or ti#1300)";
                            LOG(FATAL);
                        }
                        check_key = std::get<0>(*itr);
                    }

                    for (auto& i : kv) {
                        std::string k(std::get<0>(i));
                        VLOG(40) << "remove  " << hexstr(k);
                        status ret = remove(token, test_storage_name, k);
                        if (ret != status::OK) {
                            ASSERT_EQ(ret, status::OK);
                            std::abort();
                        }
                    }
                }
                for (auto& i : kv) {
                    std::string k(std::get<0>(i));
                    valtype v(std::get<1>(i));
                    VLOG(40) << "insert2 " << hexstr(k);
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
            valtype v(value(j));
            constexpr std::size_t v_index = 1;
            ASSERT_TRUE(eq(std::get<v_index>(tuple_list.at(j)), v));
        }

        destroy();
    }
}

TEST_F(multi_thread_put_delete_iscan_many_interior_test, DISABLED_many_interior) { // NOLINT
    /**
     * concurrent put/delete/scan in the state between none to many split of
     * interior.
     */
    many_interior_comm<std::string, value_varchar, eq_varchar, false>();
}

TEST_F(multi_thread_put_delete_iscan_many_interior_test, many_interior_inlined_value) { // NOLINT
    // inlined value version of many_interior
    many_interior_comm<pointer, value_inlined, eq_inlined, false>();
    many_interior_comm<pointer, value_inlined, eq_inlined, true>();
}

TEST_F(multi_thread_put_delete_iscan_many_interior_test, // NOLINT
       many_interior_inlined_shuffle) {
    many_interior_comm<pointer, value_inlined, eq_inlined, false, true>();
    many_interior_comm<pointer, value_inlined, eq_inlined, true, true>();
}

} // namespace yakushima::testing
