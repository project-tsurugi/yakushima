/**
 * @file multi_thread_put_delete_scan_test.cpp
 */

#include <algorithm>
#include <array>
#include <bitset>
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
constexpr static std::size_t data_size = remaining_size * key_slice_length;

auto make_key(std::size_t i) {
    std::ostringstream ss;
    ss << std::hex << std::setw(2) << std::setfill('0') << i;
    return ss.str();
}

std::size_t key_number(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

static void test_sub(std::size_t num_scan_th, const std::bitset<data_size>& delete_target, std::size_t repeats) {
    // setup
    //   insert 0, 1, ..., 119 (8*15 nodes) sequentially
    //   -> made 0-7, 8-15, 16-23, ..., 112-119
    // test
    //   scan thread(1-many) : full scan
    //   delete thread(15) for each border : delete some entries (pointed by delete_target)
    //   check: scan result is not broken:
    //            strictly ascending
    //            contains all of non delete_target entries (-> missing entry must in delete_target)

    std::size_t num_del_th = key_slice_length;

    auto scan_work = [&delete_target](std::size_t th_id) {
        Token token{};
        while (enter(token) != status::OK) { _mm_pause(); }
        for (std::size_t j = 0; j < 5; ++j) {
            std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list; // NOLINT
            ASSERT_OK(scan<char>(test_storage_name, {}, scan_endpoint::INF, {}, scan_endpoint::INF, tuple_list));
            if (tuple_list.empty()) { continue; }
            std::ostringstream ss{};
            std::string check_key{};
            std::size_t check_i{};
            std::size_t wrong_order = 0U;
            std::size_t missing = 0U;
            {
                const std::string& head_k = std::get<0>(*tuple_list.begin());
                std::size_t head_i = key_number(head_k);
                for (std::size_t i = 0; i < head_i; i++) {
                    if (!delete_target.test(i)) { // skipped entry is not in delete_target; BAD
                        ss << " !(" << make_key(i) << ")";
                        missing++;
                    }
                }
                ss << " " << head_k;
                check_key = head_k;
                check_i = head_i;
            }
            for (auto itr = tuple_list.begin() + 1; itr != tuple_list.end(); ++itr) { // NOLINT
                const std::string& itr_k = std::get<0>(*itr);
                std::size_t itr_i = key_number(itr_k);
                for (std::size_t i = check_i + 1; i < itr_i; i++) {
                    if (!delete_target.test(i)) { // skipped entry is not in delete_target; BAD
                        ss << " !(" << make_key(i) << ")";
                        missing++;
                    }
                }
                ss << " " << itr_k;
                if (check_key >= itr_k) {
                    wrong_order++;
                    ss << "*";
                }
                check_key = itr_k;
                check_i = itr_i;
            }
            {
                for (std::size_t i = check_i + 1; i < data_size; i++) {
                    if (!delete_target.test(i)) { // skipped entry is not in delete_target; BAD
                        ss << " !(" << make_key(i) << ")";
                        missing++;
                    }
                }
            }
            if (wrong_order > 0 || missing > 0) {
                LOG(INFO) << "found wrong order by th:" << th_id;
                ASSERT_EQ(wrong_order + missing, 0)
                        << "wrong_order:" << wrong_order << ", missing:" << missing
                        << ", keys[" << ss.str() << " ]\n" << delete_target;
                LOG(FATAL);
            }
        }
        leave(token);
    };

    auto del_work = [&delete_target](std::size_t th_id) {
        Token token{};
        while (enter(token) != status::OK) { _mm_pause(); }
        for (std::size_t i = th_id * remaining_size; i < (th_id + 1) * remaining_size; i++) {
            if (delete_target.test(i)) {
                auto k = make_key(i);
                ASSERT_OK(remove(token, test_storage_name, k)) << k;
            }
        }
        leave(token);
    };

    for (std::size_t r = 0; r < repeats; ++r) {
        create_storage(test_storage_name);
        {
            // setup initial tree
            Token token{};
            while (enter(token) != status::OK) { _mm_pause(); }
            for (std::size_t i = 0; i < remaining_size * key_slice_length; i++) {
                auto k = make_key(i);
                ASSERT_OK(put<char>(token, test_storage_name, k, k.data(), k.size())) << k;
            }
            leave(token);
        }

        std::vector<std::thread> thv;
        thv.reserve(num_del_th + num_scan_th);
        for (std::size_t i_d = 0, i_s = 0; i_d + i_s < num_del_th + num_scan_th; ) {
            if (i_s < num_scan_th) { thv.emplace_back(scan_work, i_s++); }
            if (i_d < num_del_th)  { thv.emplace_back(del_work, i_d++); }
        }
        for (auto&& th : thv) { th.join(); }
        thv.clear();

        destroy();
    }
}

TEST_F(multi_thread_delete_scan_test, delete_border_leaf_during_scan_i1) { // NOLINT
    std::bitset<data_size> delete_target;
    for (std::size_t i = 0; i < key_slice_length; i++) {
        delete_target.set(i * remaining_size + 1, true); // index 1 of each border node
    }
    test_sub(5, delete_target, 1);
}

TEST_F(multi_thread_delete_scan_test, delete_border_leaf_during_scan_all) { // NOLINT
    std::bitset<data_size> delete_target;
    for (std::size_t i = 0; i < data_size; i++) {
        delete_target.set(i, true); // all
    }
    test_sub(5, delete_target, 1);
}

} // namespace yakushima::testing
