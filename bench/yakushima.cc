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

#include <algorithm>
#include <random>
#include <thread>
#include <vector>

// yakushima
#include "kvs.h"

// yakushima/src/
#include "atomic_wrapper.h"
#include "clock.h"
#include "cpu.h"

// yakushima/bench/include
#include "affinity.h"
#include "random.h"
#include "tsc.h"
#include "zipf.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

using namespace yakushima;

DEFINE_uint64(duration, 3, "Duration of benchmark in seconds.");
DEFINE_uint64(get_initial_record, 1000, "# initial key-values for get bench");
DEFINE_double(get_skew, 0.0, "access skew of get operations.");
DEFINE_string(instruction, "get", "put or get. The default is insert.");
DEFINE_uint64(thread, 1, "# worker threads.");
DEFINE_uint32(value_size, 4, "value size");

std::atomic<bool> Failure{false};

static void check_flags() {
  std::cout << "parameter settings\n"
            << "duration :\t\t" << FLAGS_duration << "\n"
            << "get_initial_record :\t" << FLAGS_get_initial_record << "\n"
            << "get_skew :\t\t" << FLAGS_get_skew << "\n"
            << "instruction :\t\t" << FLAGS_instruction << "\n"
            << "thread :\t\t" << FLAGS_thread << "\n"
            << "value_size :\t\t" << FLAGS_value_size << std::endl;

  if (FLAGS_thread == 0) {
    std::cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }

  if (FLAGS_get_initial_record == 0 && FLAGS_instruction == "get") {
    std::cerr << "It can't execute get bench against 0 size table." << std::endl;
    exit(1);
  }

  if (FLAGS_duration == 0) {
    std::cerr << "Duration of benchmark in seconds must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_instruction != "insert" && FLAGS_instruction != "put" && FLAGS_instruction != "get") {
    std::cerr << "The instruction option must be insert or put or get. The default is insert." << std::endl;
    exit(1);
  }

  if (FLAGS_get_skew < 0 || FLAGS_get_skew > 1) {
    std::cerr << "access skew must be in the range 0 to 0.999..." << std::endl;
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

void parallel_build_tree() {
  struct S {
    static void parallel_build_worker(std::uint64_t left_edge, std::uint64_t right_edge) {
      Token token;
      masstree_kvs::enter(token);
      std::string value(FLAGS_value_size, '0');
      for (std::size_t i = left_edge; i < right_edge; ++i) {
        std::string key{reinterpret_cast<char *>(&i), sizeof(std::uint64_t)};
        masstree_kvs::put(std::string_view(key), value.data(), value.size());
      }
      masstree_kvs::leave(token);
    }
  };

  if (FLAGS_get_initial_record < 1000) {
    // small tree is built by single thread.
    std::cout << "parallel_build_tree : concurrency : 1" << std::endl;
    S::parallel_build_worker(0, FLAGS_get_initial_record);
  } else {
    // large tree is built by multi thread.
    std::cout << "parallel_build_tree : concurrency : " << std::thread::hardware_concurrency() << std::endl;
    std::vector<std::thread> thv;
    for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
      if (i != std::thread::hardware_concurrency() - 1) {
        thv.emplace_back(S::parallel_build_worker, FLAGS_get_initial_record / std::thread::hardware_concurrency() * i,
                         FLAGS_get_initial_record / std::thread::hardware_concurrency() * (i + 1));
      } else {
        // if FLAGS_get_initial_record is odd number and hardware_concurrency() is even number, you should care surplus.
        thv.emplace_back(S::parallel_build_worker, FLAGS_get_initial_record / std::thread::hardware_concurrency() * i,
                         FLAGS_get_initial_record);
      }
    }
    for (auto &th : thv) {
      th.join();
    }
  }
}

void get_worker(const size_t thid, char &ready, const bool &start, const bool &quit, std::size_t &res) {
  // init work
  Xoroshiro128Plus rnd;
  FastZipf zipf(&rnd, FLAGS_get_skew, FLAGS_get_initial_record);

  // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
  set_thread_affinity(thid);
#endif

  storeReleaseN(ready, 1);
  while (!loadAcquireN(start)) _mm_pause();

  Token token;
  masstree_kvs::enter(token);
  std::uint64_t local_res{0};
  while (!loadAcquireN(quit)) {
    uint64_t keynm = zipf() % FLAGS_get_initial_record;
    std::string key{reinterpret_cast<char *>(&keynm), sizeof(std::uint64_t)};
    std::tuple<char *, std::size_t> ret = masstree_kvs::get<char>(std::string_view(key));
    if (std::get<0>(ret) == nullptr) {
      Failure.store(true, std::memory_order_release);
    } else {
      if (Failure.load(std::memory_order_acquire)) break;
    }
    ++local_res;
  }
  masstree_kvs::leave(token);
  res = local_res;
}

void put_worker(const size_t thid, char &ready, const bool &start, const bool &quit, std::size_t &res) {
  // this function can be used in Linux environment only.
#ifdef YAKUSHIMA_LINUX
  set_thread_affinity(thid);
#endif

  Token token;
  masstree_kvs::enter(token);
  std::uint64_t left_edge(UINT64_MAX / FLAGS_thread * thid),
          right_edge(UINT64_MAX / FLAGS_thread * (thid + 1));
  std::string value(FLAGS_value_size, '0');

  storeReleaseN(ready, 1);
  while (!loadAcquireN(start)) _mm_pause();

  std::uint64_t local_res{0};
  for (std::uint64_t i = left_edge; i < right_edge; ++i) {
    std::string key{reinterpret_cast<char *>(&i), sizeof(std::uint64_t)};
    masstree_kvs::put(std::string_view(key), value.data(), value.size());
    ++local_res;
    if (i == right_edge - 1) {
      Failure.store(true, std::memory_order_release);
    } else {
      if (Failure.load(std::memory_order_acquire) ||
          loadAcquireN(quit)) {
        break;
      }
    }
  }

  // parallel delete existing tree to reduce deleting time by single thread.
  for (std::uint64_t i = left_edge; i < left_edge + local_res; ++i) {
    std::string key{reinterpret_cast<char *>(&i), sizeof(std::uint64_t)};
    masstree_kvs::remove(token, std::string_view(key));
  }

  masstree_kvs::leave(token);
  res = local_res;
}

static void invoke_leader() {
  alignas(CACHE_LINE_SIZE) bool start = false;
  alignas(CACHE_LINE_SIZE) bool quit = false;
  alignas(CACHE_LINE_SIZE) std::vector<std::size_t> res(FLAGS_thread);

  std::cout << "[start] init masstree database." << std::endl;
  masstree_kvs::init();
  std::cout << "[end] init masstree database." << std::endl;

  std::cout << "[report] This experiments use ";
  if (FLAGS_instruction == "get") {
    std::cout << "get" << std::endl;
    // build initial tree
    std::cout << "[start] parallel build initial tree." << std::endl;
    parallel_build_tree();
    std::cout << "[end] parallel build initial tree." << std::endl;
  } else if (FLAGS_instruction == "put") {
    std::cout << "put" << std::endl;
  } else {
    std::cout << "[error] invalid instruction." << std::endl;
    exit(1);
  }

  std::vector<char> readys(FLAGS_thread);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < FLAGS_thread; ++i) {
    if (FLAGS_instruction == "get") {
      thv.emplace_back(get_worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res[i]));
    } else if (FLAGS_instruction == "put") {
      thv.emplace_back(put_worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(res[i]));
    }
  }

  waitForReady(readys);
  storeReleaseN(start, true);
  std::cout << "[start] measurement." << std::endl;
  for (size_t i = 0; i < FLAGS_duration; ++i) {
    sleepMs(1000);
    if (Failure.load(std::memory_order_acquire)) break;
  }
  std::cout << "[end] measurement." << std::endl;
  storeReleaseN(quit, true);
  std::cout << "[start] join worker threads." << std::endl;
  for (auto &th : thv) th.join();
  std::cout << "[end] join worker threads." << std::endl;

  if (Failure.load(std::memory_order_acquire)) {
    if (FLAGS_instruction == "put") {

      std::cout << __FILE__ << " : " << __LINE__ << " : experimental setting is bad, which leads to overflow.\n"
                << "please set less durations." << std::endl;
    } else if (FLAGS_instruction == "get") {
      std::cout << __FILE__ << " : " << __LINE__ << "fatal error." << std::endl;
    }
    exit(1);
  }

  /**
   * get test : read records.
   * put test : put records.
   */
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

  std::cout << "[start] fin masstree." << std::endl;
  masstree_kvs::fin();
  std::cout << "[end] fin masstree." << std::endl;
}

int main(int argc, char *argv[]) {
  std::cout << "start yakushima bench." << std::endl;
  gflags::SetUsageMessage("micro-benchmark for yakushima");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  check_flags();
  invoke_leader();

  return 0;
}
