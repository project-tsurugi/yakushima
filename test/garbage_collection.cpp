/**
 * @file garbage_collection.cpp
 */

#include <array>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class garbage_collection : public ::testing::Test {
protected:
  void SetUp() override {
    masstree_kvs::init();
  }

  void TearDown() override {
    masstree_kvs::fin();
  }
};

TEST_F(garbage_collection, gc) { // NOLINT
  constexpr std::size_t th_num = 5;
  std::array<Token, th_num> token{};
  constexpr std::size_t array_size = 40;
  struct S {
    static void work(std::array<std::tuple<std::string, std::string>, array_size> &kv, Token &token) {
      for (std::size_t j = 0; j < 10; ++j) {
        masstree_kvs::enter(token);
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          status ret = masstree_kvs::put(k, v.data(), v.size());
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
        for (auto &i : kv) {
          std::string k(std::get<0>(i));
          std::string v(std::get<1>(i));
          status ret = masstree_kvs::remove(token, k);
          if (ret != status::OK) {
            ASSERT_EQ(ret, status::OK);
            std::abort();
          }
        }
        masstree_kvs::leave(token);
      }
    }
  };

  std::array<std::array<std::tuple<std::string, std::string>, array_size>, th_num> kv{};
  for (std::size_t i = 0; i < th_num; ++i) {
    for (std::size_t j = 0; j < array_size; ++j) {
      kv.at(i).at(j) = {std::to_string(i * array_size + j), std::string("value")};
    }
  }

  std::array<std::thread, th_num> th_array{};
  for (std::size_t i = 0; i < th_num; ++i) {
    th_array.at(i) = std::thread(S::work, std::ref(kv.at(i)), std::ref(token.at(i)));
  }

  for (std::size_t i = 0; i < th_num; ++i) {
    th_array.at(i).join();
  }
}

}  // namespace yakushima::testing
