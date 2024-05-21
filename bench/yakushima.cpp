/*
 * Copyright 2019-2024 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>
#include <boost/config/helper_macros.hpp>

// yakushima
#include "kvs.h"

// yakushima/include/
#include "atomic_wrapper.h"
#include "clock.h"
#include "cpu.h"

// yakushima/bench/include
#include "affinity.h"
#include "memory.h"
#include "random.h"
#include "tsc.h"
#include "zipf.h"

#ifdef PERFORMANCE_TOOLS
// sandbox-performance-tools
#include <performance-tools/perf_counter.h>

#include <performance-tools/lap_counter.h>
#include <performance-tools/lap_counter_init.h>
#endif

#include "gflags/gflags.h"
#include "glog/logging.h"

using namespace yakushima;

DEFINE_uint64(duration, 3, "Duration of benchmark in seconds."); // NOLINT
DEFINE_uint64(initial_record, 1000,                              // NOLINT
              "# initial key-values for get bench");             // NOLINT
DEFINE_double(get_skew, 0.0, "access skew of get operations.");  // NOLINT
DEFINE_string(instruction, "get",                                // NOLINT
              "put or get. The default is insert.");             // NOLINT
DEFINE_uint64(thread, 1, "# worker threads. max: " BOOST_STRINGIZE(YAKUSHIMA_MAX_PARALLEL_SESSIONS));
DEFINE_uint32(value_size, 8, "value size");                      // NOLINT

// unique for instruction
DEFINE_uint64(range_of_scan, 1000, "# elements of range."); // NOLINT

std::string bench_storage{"1"}; // NOLINT

static void check_flags() {
    std::cout << "parameter settings\n"
              << "duration :\t\t" << FLAGS_duration << "\n"
              << "initial_record :\t" << FLAGS_initial_record << "\n"
              << "get_skew :\t\t" << FLAGS_get_skew << "\n"
              << "instruction :\t\t" << FLAGS_instruction << "\n"
              << "thread :\t\t" << FLAGS_thread << "\n"
              << "range_of_scan :\t\t" << FLAGS_range_of_scan << "\n"
              << "value_size :\t\t" << FLAGS_value_size << std::endl;

    // about thread
    if (FLAGS_thread == 0) {
        LOG(FATAL) << "Number of threads must be larger than 0.";
    }
    if (FLAGS_thread > YAKUSHIMA_MAX_PARALLEL_SESSIONS) {
        LOG(FATAL) << "Number of threads is too big, max: " << YAKUSHIMA_MAX_PARALLEL_SESSIONS;
    }

    // about duration
    if (FLAGS_duration == 0) {
        LOG(FATAL) << "Duration of benchmark in seconds must be larger than 0.";
    }

    // about instruction
    if (FLAGS_instruction != "put" && FLAGS_instruction != "get" &&
        FLAGS_instruction != "remove" && FLAGS_instruction != "scan") {
        LOG(FATAL)
                << "The instruction option must be put, remove, scan or get. "
                   "The default is get.";
    }

    // about skew
    if (FLAGS_get_skew < 0 || FLAGS_get_skew > 1) {
        LOG(FATAL) << "access skew must be in the range 0 to 0.999...";
    }
    // about get / remove bench
    if (FLAGS_initial_record == 0 &&
        (FLAGS_instruction == "get" || FLAGS_instruction == "remove" ||
         FLAGS_instruction == "scan")) {
        LOG(FATAL) << "It can't execute get / remove / scan bench against 0 "
                      "size table.";
    }

    // about scan bench
    if (FLAGS_range_of_scan > FLAGS_initial_record) {
        LOG(FATAL) << "It can't execute larger range against entire. Please set"
                      "less range than entire.";
    }
}

static bool isReady(const std::vector<char>& readys) {
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) { _mm_pause(); }
}

void parallel_build_tree() {
    struct S {
        static void parallel_build_worker(std::uint64_t left_edge,
                                          std::uint64_t right_edge) {
            Token token{};
            while (enter(token) != status::OK) { _mm_pause(); }
            std::string value(FLAGS_value_size, '0');
            for (std::uint64_t i = left_edge; i < right_edge; ++i) {
                void* p = (&i);
                std::string key{
                        static_cast<char*>(p),
                        sizeof(std::uint64_t)}; // sizeof(std::size_t) points to loop variable.
                put(token, bench_storage, key, value.data(), value.size());
            }
            leave(token);
        }
    };

    if (FLAGS_initial_record < 1000) {
        // small tree is built by single thread.
        std::cout << "parallel_build_tree : concurrency : 1" << std::endl;
        S::parallel_build_worker(0, FLAGS_initial_record);
    } else {
        // large tree is built by multi thread.
        std::cout << "parallel_build_tree : concurrency : "
                  << std::thread::hardware_concurrency() << std::endl;
        std::vector<std::thread> thv;
        for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
            if (i != std::thread::hardware_concurrency() - 1) {
                thv.emplace_back(
                        S::parallel_build_worker,
                        FLAGS_initial_record /
                                std::thread::hardware_concurrency() * i,
                        FLAGS_initial_record /
                                std::thread::hardware_concurrency() * (i + 1));
            } else {
                // if FLAGS_initial_record is odd number and hardware_concurrency() is even
                // number, you should care surplus.
                thv.emplace_back(S::parallel_build_worker,
                                 FLAGS_initial_record /
                                         std::thread::hardware_concurrency() *
                                         i,
                                 FLAGS_initial_record);
            }
        }
        for (auto& th : thv) { th.join(); }
    }
}

void get_worker(const size_t thid, char& ready, const bool& start,
                const bool& quit, std::size_t& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, FLAGS_get_skew, FLAGS_initial_record);

    // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
    set_thread_affinity(static_cast<const int>(thid));
#endif

    storeReleaseN(ready, 1);
    while (!loadAcquireN(start)) _mm_pause();

    Token token{};
    while (enter(token) != status::OK) { _mm_pause(); }
    std::uint64_t local_res{0};
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(0, thid);
#endif
    while (!loadAcquireN(quit)) {
        std::uint64_t keynm = zipf() % FLAGS_initial_record;
        void* p = (&keynm);
        std::string key{static_cast<char*>(p), sizeof(std::uint64_t)};
        std::pair<char*, std::size_t> ret{};
        if (get<char>(bench_storage, key, ret) != status::OK) {
            LOG(ERROR) << "fatal error";
        }
        ++local_res;
    }
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(1);
#endif
    leave(token);
    res = local_res;
}

void scan_worker(const size_t thid, char& ready, const bool& start,
                 const bool& quit, std::size_t& res) {
    // init work
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, FLAGS_get_skew, FLAGS_initial_record);

    // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
    set_thread_affinity(static_cast<const int>(thid));
#endif

    storeReleaseN(ready, 1);
    while (!loadAcquireN(start)) _mm_pause();

    Token token{};
    while (enter(token) != status::OK) { _mm_pause(); }
    std::uint64_t local_res{0};
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(0, thid);
#endif
    while (!loadAcquireN(quit)) {
        std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list{};
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        if (scan(bench_storage, "", scan_endpoint::INF, "", scan_endpoint::INF,
                 tuple_list, &nv, FLAGS_range_of_scan) != status::OK) {
            LOG(ERROR) << "fatal error";
        }
        if (tuple_list.size() != FLAGS_range_of_scan) {
            LOG(FATAL) << "programming error: " << tuple_list.size();
        }
        ++local_res;
    }
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(1);
#endif
    leave(token);
    res = local_res;
}

void remove_worker(const size_t thid, char& ready, const bool& start,
                   const bool& quit, std::size_t& res) {
    // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
    set_thread_affinity(static_cast<const int>(thid));
#endif

    Token token{};
    while (enter(token) != status::OK) { _mm_pause(); }
    std::uint64_t left_edge(FLAGS_initial_record / FLAGS_thread * thid);
    std::uint64_t right_edge(FLAGS_initial_record / FLAGS_thread * (thid + 1));
    std::string value(FLAGS_value_size, '0');

    storeReleaseN(ready, 1);
    while (!loadAcquireN(start)) { _mm_pause(); }

    std::uint64_t local_res{0};
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(0, thid);
#endif
    for (std::uint64_t i = left_edge; i < right_edge; ++i) {
        std::string key{reinterpret_cast<char*>(&i), // NOLINT
                        sizeof(std::uint64_t)};      // NOLINT
        try {
            auto rc = remove(token, bench_storage, key);
            if (rc != status::OK) { LOG(FATAL) << "unexpected error."; }
        } catch (std::bad_alloc&) {
            LOG(FATAL) << "bad_alloc. Please set less duration.";
        }
        ++local_res;
        if (loadAcquireN(quit)) { break; }
        if (i == right_edge - 1) {
            LOG(FATAL) << "This experiments fails. Please set less duration or "
                          "larger initial record.";
        }
    }
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(1, thid);
#endif

    leave(token);
    res = local_res;
}
void put_worker(const size_t thid, char& ready, const bool& start,
                const bool& quit, std::size_t& res) {
    // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
    set_thread_affinity(static_cast<const int>(thid));
#endif

    Token token{};
    while (enter(token) != status::OK) { _mm_pause(); }
    std::uint64_t left_edge(UINT64_MAX / FLAGS_thread * thid);
    std::uint64_t right_edge(UINT64_MAX / FLAGS_thread * (thid + 1));
    std::string value(FLAGS_value_size, '0');

    storeReleaseN(ready, 1);
    while (!loadAcquireN(start)) { _mm_pause(); }

    std::uint64_t local_res{0};
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(0, thid);
#endif
    for (std::uint64_t i = left_edge; i < right_edge; ++i) {
        std::string key{reinterpret_cast<char*>(&i), // NOLINT
                        sizeof(std::uint64_t)};      // NOLINT
        try {
            put(token, bench_storage, key, value.data(), value.size());
        } catch (std::bad_alloc&) {
            LOG(FATAL) << "bad_alloc. Please set less duration.";
        }
        ++local_res;
        if (loadAcquireN(quit)) { break; }
        if (i == right_edge - 1) {
            LOG(FATAL) << "This experiments fails. Please set less duration.";
        }
    }
#ifdef PERFORMANCE_TOOLS
    performance_tools::get_watch().set_point(1, thid);
#endif

    leave(token);
    res = local_res;
}

static void invoke_leader() try {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<std::size_t> res(FLAGS_thread);

    LOG(INFO) << "[start] init masstree database.";
    init();
    create_storage(bench_storage);
    LOG(INFO) << "[end] init masstree database.";

    std::cout << "[report] This experiments use ";
    if (FLAGS_instruction == "get" || FLAGS_instruction == "remove" ||
        FLAGS_instruction == "scan") {
        std::cout << FLAGS_instruction << std::endl;
        // build initial tree
        LOG(INFO) << "[start] parallel build initial tree.";
        parallel_build_tree();
        LOG(INFO) << "[end] parallel build initial tree.";
    } else if (FLAGS_instruction == "put") {
        std::cout << "put" << std::endl;
    } else {
        LOG(FATAL) << "[error] invalid instruction.";
    }

    std::vector<char> readys(FLAGS_thread);
    std::vector<std::thread> thv;
    for (size_t i = 0; i < FLAGS_thread; ++i) {
        if (FLAGS_instruction == "get") {
            thv.emplace_back(get_worker, i, std::ref(readys[i]),
                             std::ref(start), std::ref(quit), std::ref(res[i]));
        } else if (FLAGS_instruction == "put") {
            thv.emplace_back(put_worker, i, std::ref(readys[i]),
                             std::ref(start), std::ref(quit), std::ref(res[i]));
        } else if (FLAGS_instruction == "remove") {
            thv.emplace_back(remove_worker, i, std::ref(readys[i]),
                             std::ref(start), std::ref(quit), std::ref(res[i]));
        } else if (FLAGS_instruction == "scan") {
            thv.emplace_back(scan_worker, i, std::ref(readys[i]),
                             std::ref(start), std::ref(quit), std::ref(res[i]));
        } else {
            LOG(FATAL) << "invalid instruction type.";
        }
    }

    waitForReady(readys);
    storeReleaseN(start, true);
    LOG(INFO) << "[start] measurement.";
    for (size_t i = 0; i < FLAGS_duration; ++i) { sleepMs(1000); }
    LOG(INFO) << "[end] measurement.";
    storeReleaseN(quit, true);
    LOG(INFO) << "[start] join worker threads.";
    for (auto& th : thv) { th.join(); }
    LOG(INFO) << "[end] join worker threads.";

    /**
      * get test : read records.
      * put test : put records.
      */
    std::uint64_t fin_res{0};
    for (std::uint64_t i = 0; i < FLAGS_thread; ++i) {
        if ((UINT64_MAX - fin_res) < res[i]) {
            LOG(FATAL)
                    << "experimental setting is bad, which leads to overflow.";
        }
        fin_res += res[i];
    }
    std::cout << "throughput[ops/s]: " << fin_res / FLAGS_duration << std::endl;
    displayRusageRUMaxrss();
    LOG(INFO) << "[start] fin masstree.";
    std::chrono::system_clock::time_point c_start;
    std::chrono::system_clock::time_point c_end;
    c_start = std::chrono::system_clock::now();
    fin();
    c_end = std::chrono::system_clock::now();
    LOG(INFO) << "[end] fin masstree.";
    std::cout << "cleanup_time[ms]: "
              << static_cast<double>(
                         std::chrono::duration_cast< // NOLINT
                                 std::chrono::microseconds>(c_end - c_start)
                                 .count() /
                         1000.0)
              << std::endl;

#ifdef PERFORMANCE_TOOLS
    performance_tools::counter_class sum{};
    for (auto&& r : *performance_tools::get_watch().laps(0, 1)) { sum += r; }
    sum /= ((fin_res + (1000000 / 2)) / 1000000);
    std::cout << "performance_counter: " << sum << " fin_res: " << fin_res
              << std::endl;
#endif
} catch (...) { LOG(FATAL) << "catch exception"; }

int main(int argc, char* argv[]) {
    // about glog
    google::InitGoogleLogging("yakushima-bench-yakushima");
    FLAGS_stderrthreshold = 0;
    std::cout << "start yakushima bench." << std::endl;

    // about gflags
    gflags::SetUsageMessage(
            static_cast<const std::string&>("micro-benchmark for yakushima"));
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // start bench
    check_flags();
    invoke_leader();

    return 0;
}