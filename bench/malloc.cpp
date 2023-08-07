/*
 * Copyright 2019-2020 tsurugi project.
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

#include <xmmintrin.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

// yakushima/src/
#include "atomic_wrapper.h"
#include "clock.h"
#include "cpu.h"

// yakushima/bench/include
#include "affinity.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

using namespace yakushima;

DEFINE_uint64(alloc_size, 4, "allocation size.");                // NOLINT
DEFINE_uint64(duration, 3, "Duration of benchmark in seconds."); // NOLINT
DEFINE_uint64(thread, 1, "# worker threads.");                   // NOLINT

static void check_flags() {
    std::cout << "parameter settings\n"
              << "duration :\t\t" << FLAGS_duration << "\n"
              << "thread :\t\t" << FLAGS_thread << "\n"
              << std::endl;

    if (FLAGS_thread == 0) {
        std::cerr << "Number of threads must be larger than 0." << std::endl;
        exit(1);
    }

    if (FLAGS_duration == 0) {
        std::cerr << "Duration of benchmark in seconds must be larger than 0."
                  << std::endl;
        exit(1);
    }
}

static bool isReady(const std::vector<char>& readys) {
    return std::all_of(readys.begin(), readys.end(),
                       [](char b) { return b != 0; });
}

static void waitForReady(const std::vector<char>& readys) {
    while (!isReady(readys)) { _mm_pause(); }
}

void worker(const size_t thid, char& ready, const bool& start, const bool& quit,
            std::size_t& res) {
    // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
    set_thread_affinity(static_cast<int>(thid));
#endif

    /**
   * tools used in experiments.
   */
    std::uint64_t local_res{0};

    /**
   * initialize source array.
   */
    std::unique_ptr<char[]> own_str(new char[FLAGS_alloc_size]); // NOLINT
    /**
   * c-style array NOLINT reasons : It wants to decide array size after compile. So it
   * wants to use c-style array.
   */
    for (std::size_t i = 0; i < FLAGS_alloc_size; ++i) {
        /**
     * thid means that it creates unique str among thread.
     * If you use more threads than CHAR_MAX, rewrite this part.
     */
        own_str.get()[i] = static_cast<char>(thid);
    }

    storeReleaseN(ready, 1);
    while (!loadAcquireN(start)) _mm_pause();

    std::vector<std::unique_ptr<char[]>> vec; // NOLINT
    while (!loadAcquireN(quit)) {
        std::unique_ptr<char[]> str(new char[FLAGS_alloc_size]); // NOLINT
        std::memcpy(str.get(), own_str.get(), FLAGS_alloc_size);
        vec.emplace_back(std::move(str));
        ++local_res;
    }
    res = local_res;
}

static void invoke_leader() {
    alignas(CACHE_LINE_SIZE) bool start = false;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    alignas(CACHE_LINE_SIZE) std::vector<std::size_t> res(FLAGS_thread);

    std::vector<char> readys(FLAGS_thread);
    std::vector<std::thread> thv;
    for (size_t i = 0; i < FLAGS_thread; ++i) {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                         std::ref(quit), std::ref(res[i]));
    }

#ifdef ENABLE_JEMALLOC
    std::cout << "[report] use jemalloc." << std::endl;
#endif

    waitForReady(readys);
    storeReleaseN(start, true);
    std::cout << "[start] measurement." << std::endl;
    for (size_t i = 0; i < FLAGS_duration; ++i) { sleepMs(1000); }
    std::cout << "[end] measurement." << std::endl;
    storeReleaseN(quit, true);
    std::cout << "[start] join worker threads." << std::endl;
    for (auto& th : thv) th.join();
    std::cout << "[end] join worker threads." << std::endl;

    std::uint64_t fin_res{0};
    for (std::uint64_t i = 0; i < FLAGS_thread; ++i) {
        if ((UINT64_MAX - fin_res) < res[i]) {
            std::cout << __FILE__ << " : " << __LINE__
                      << " : experimental setting is bad, which leads to "
                         "overflow."
                      << std::endl;
            exit(1);
        }
        fin_res += res[i];
    }
    std::cout << "throughput[ops/s]: " << fin_res / FLAGS_duration << std::endl;
}

int main(int argc, char* argv[]) {
    std::cout << "start yakushima bench." << std::endl;
    gflags::SetUsageMessage(
            static_cast<const std::string&>("micro-benchmark for yakushima"));
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    check_flags();
    invoke_leader();

    return 0;
}
