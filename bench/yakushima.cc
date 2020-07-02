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

// yakushima
#include "kvs.h"

// yakushima/src/
#include "atomic_wrapper.h"
#include "clock.h"
#include "cpu.h"

// yakushima/bench/include
#include "random.h"
#include "zipf.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

using namespace yakushima;

DEFINE_uint64(cpumhz, 2100, "# cpu MHz of execution environment. It is used measuring some time.");
DEFINE_uint64(get_duration, 3, "Duration of get benchmark in seconds.");
DEFINE_uint64(initial_record, 1000000, "# initial key-values for get bench");
DEFINE_string(instruction, "get", "put or get. The default is insert.");
DEFINE_uint64(put_records, 1000000, "# records for put bench");
DEFINE_double(skew, 0.0, "access skew of transaction.");
DEFINE_uint64(thread, 1, "# worker threads.");

static void check_flags() {
  if (FLAGS_thread == 0) {
    std::cerr << "Number of threads must be larger than 0." << std::endl;
    exit(1);
  }

  if (FLAGS_initial_record == 0 && FLAGS_instruction == "get") {
    std::cerr << "It can't execute get bench against 0 size table." << std::endl;
    exit(1);
  }

  if (FLAGS_cpumhz == 0) {
    std::cerr << "CPU MHz of execution environment. It is used measuring some time. "
            "It must be larger than 0."
         << std::endl;
    exit(1);
  }
  if (FLAGS_get_duration == 0) {
    std::cerr << "Duration of benchmark in seconds must be larger than 0." << std::endl;
    exit(1);
  }
  if (FLAGS_instruction != "insert" && FLAGS_instruction != "put" && FLAGS_instruction != "get") {
    std::cerr << "The instruction option must be insert or put or get. The default is insert." << std::endl;
    exit(1);
  }

  if (FLAGS_skew < 0 || FLAGS_skew > 1) {
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

void get_worker(const size_t thid, char &ready, const bool &start, const bool &quit, std::size_t &res) {
  // init work
  Xoroshiro128Plus rnd;
  FastZipf zipf(&rnd, FLAGS_skew, FLAGS_initial_record);
  std::size_t local_res{0};

  // this function can be used in Linux environment only.
#ifdef yakushima_Linux
  setThreadAffinity(thid);
#endif

  storeReleaseN(ready, 1);
  while (!loadAcquireN(start)) _mm_pause();

  Token token;
  masstree_kvs::enter(token);
  while (!loadAcquireN(quit)) {
#if 0
    uint64_t keynm = zipf() % FLAGS_record;
    uint64_t keybs = __builtin_bswap64(keynm);
    kvs::Record* record;
    record = MTDB.get_value((char*)&keybs, sizeof(uint64_t));
    if (record == nullptr) ERR;
    ++myres.local_commit_counts_;
#endif
  }
  masstree_kvs::leave(token);

  res = local_res;
}

void put_worker(const size_t thid, char &ready, const bool &start, std::size_t &res) {
  // init work
  Xoroshiro128Plus rnd;
  FastZipf zipf(&rnd, FLAGS_skew, FLAGS_initial_record);
  std::size_t local_res{0};

  // this function can be used in Linux environment only.
#ifdef yakushima_Linux
  setThreadAffinity(thid);
#endif

  storeReleaseN(ready, 1);
  while (!loadAcquireN(start)) _mm_pause();

  Token token;
  masstree_kvs::enter(token);
#if 0
  while (!loadAcquireN(quit)) {
    std::size_t start(UINT64_MAX / FLAGS_thread * thid),
            end(UINT64_MAX / FLAGS_thread * (thid + 1));
    for (auto i = start; i < end; ++i) {
      uint64_t keybs = __builtin_bswap64(i);
      std::string value(FLAGS_val_length, '0');
      make_string(value, rnd);
      insert(token, storage, reinterpret_cast<char *>(&keybs),
             sizeof(uint64_t), value.data(), FLAGS_val_length);
      commit(token);
      ++myres.local_commit_counts_;
      if (loadAcquire(quit)) break;
    }
  }
#endif
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
  if (FLAGS_instruction == "put" || FLAGS_instruction == "get") {
    // build_initial_table(FLAGS_initial_record, FLAGS_thread);
  }

  std::cout << "[report] This experiments use ";
  if (FLAGS_instruction == "get") {
    std::cout << "get" << std::endl;
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
      thv.emplace_back(put_worker, i, std::ref(readys[i]), std::ref(start), std::ref(res[i]));
    }
  }

  waitForReady(readys);
  storeReleaseN(start, true);
  std::cout << "[start] measurement." << std::endl;
  if (FLAGS_instruction == "get") {
    for (size_t i = 0; i < FLAGS_get_duration; ++i) {
      sleepMs(1000);
    }
    std::cout << "[end] measurement." << std::endl;
    storeReleaseN(quit, true);
    std::cout << "[start] join worker threads." << std::endl;
    for (auto &th : thv) th.join();
    std::cout << "[end] join worker threads." << std::endl;
  } else if (FLAGS_instruction == "put") {
    for (auto &th : thv) th.join();
    std::cout << "[end] measurement." << std::endl;
  }

  /**
   * get test : read records.
   * put test : latency.
   */
  std::uint64_t fin_res{0};
  for (auto i = 0; i < FLAGS_thread; ++i) {
    if ((UINT64_MAX - fin_res) < res[i]) {
      std::cout << __FILE__ << " : " << __LINE__ << " : experimental setting is bad, which leads to overflow." << std::endl;
      exit(1);
    }
    fin_res += res[i];
  }
  if (FLAGS_instruction == "get") {
    std::cout << "throughput[ops/s]: " << fin_res / FLAGS_get_duration << std::endl;
  } else if (FLAGS_instruction == "put") {
    std::uint64_t clocks = fin_res / FLAGS_thread / FLAGS_put_records;
    long double l{static_cast<long double>(clocks)};
    l /= FLAGS_cpumhz;
    std::cout << "latency[s/op]: " << l << std::endl;
  }

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
