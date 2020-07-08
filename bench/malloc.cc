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

#include <cstring>
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

using namespace yakushima;

DEFINE_uint64(alloc_size, 4, "allocation size.");
DEFINE_uint64(cpumhz, 2100, "# cpu MHz of execution environment. It is used measuring some time.");
DEFINE_uint64(duration, 3, "Duration of benchmark in seconds.");
DEFINE_uint64(thread, 1, "# worker threads.");

static void check_flags() {
  std::cout << "parameter settings\n"
            << "cpumhz :\t\t" << FLAGS_cpumhz << "\n"
            << "duration :\t\t" << FLAGS_duration << "\n"
            << "thread :\t\t" << FLAGS_thread << "\n" << std::endl;

  if (FLAGS_thread == 0) {
    std::cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }

  if (FLAGS_cpumhz == 0) {
    std::cerr << "CPU MHz of execution environment. It is used measuring some time. "
                 "It must be larger than 0."
              << std::endl;
    exit(1);
  }
  if (FLAGS_duration == 0) {
    std::cerr << "Duration of benchmark in seconds must be larger than 0." << std::endl;
    exit(1);
  }
}

static bool isReady(const std::vector<char> &readys) {
  for (const char &b : readys) {
    if (!loadAcquireN(b)) return false;
  }
  return true;
}

static void waitForReady(const std::vector<char> &readys) {
  while (!isReady(readys)) {
    _mm_pause();
  }
}

void worker(const size_t thid, char &ready, const bool &start, const bool &quit, std::size_t &res) {
  // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
  set_thread_affinity(thid);
#endif

  /**
   * tools used in experiments.
   */
  std::vector<char *> vec;
  std::uint64_t local_res{0};

  /**
   * initialize source array.
   */
  char own_str[FLAGS_alloc_size];
  for (std::size_t i = 0; i < FLAGS_alloc_size; ++i) {
    /**
     * thid means that it creates unique str among thread.
     * If you use more threads than CHAR_MAX, rewrite this part.
     */
    own_str[i] = thid;
  }

  storeReleaseN(ready, 1);
  while (!loadAcquireN(start)) _mm_pause();

  while (!loadAcquireN(quit)) {
    char* str = new char[FLAGS_alloc_size];
    std::memcpy(str, own_str, FLAGS_alloc_size);
    vec.emplace_back(str);
    ++local_res;
  }
  res = local_res;

  for (auto &&elem : vec) {
    delete[] elem;
  }
}

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<std::size_t> res(FLAGS_thread);

  std::vector<char> readys(FLAGS_thread);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread; ++i) {
    thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res[i]));
  }

  waitForReady(readys);
  storeReleaseN(start, true);
  std::cout << "[start] measurement." << std::endl;
  for (size_t i = 0; i < FLAGS_duration; ++i) {
    sleepMs(1000);
  }
  std::cout << "[end] measurement." << std::endl;
  storeReleaseN(quit, true);
  std::cout << "[start] join worker threads." << std::endl;
  for (auto &th : thv) th.join();
  std::cout << "[end] join worker threads." << std::endl;

  std::uint64_t fin_res{0};
  for (auto i = 0; i < FLAGS_thread; ++i) {
    if ((UINT64_MAX - fin_res) < res[i]) {
      std::cout << __FILE__ << " : " << __LINE__ << " : experimental setting is bad, which leads to overflow."
                << std::endl;
      exit(1);
    }
    fin_res += res[i];
  }
  std::cout << "throughput[ops/s]: " << fin_res / FLAGS_duration << std::endl;
}

int main(int argc, char *argv[]) {
  std::cout << "start yakushima bench." << std::endl;
  gflags::SetUsageMessage("micro-benchmark for yakushima");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  check_flags();
  invoke_leader();

  return 0;
}
