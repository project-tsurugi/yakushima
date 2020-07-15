/**
 * @file scan_test.cpp
 */

#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class st : public ::testing::Test {
protected:
  st() {
    masstree_kvs::init();
  }

  ~st() {
    masstree_kvs::fin();
  }
};

TEST_F(st, test1) { // NOLINT
  /**
   * put one key-value
   */
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(0, 0),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(0, 0),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(0, 0),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(0, 0),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(k),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(k),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(k), false, std::string_view(0, 0),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(k), false, std::string_view(0, 0),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(k), false, std::string_view(k),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 1);
  ASSERT_EQ(std::get<1>(tuple_list.at(0)), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(0)), v.data(), v.size()), 0);
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(k), false, std::string_view(k),
                                                            true, tuple_list));
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(k), true, std::string_view(0, 0),
                                                 false, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::OK, masstree_kvs::scan<char>(std::string_view(k), true, std::string_view(0, 0),
                                                 true, tuple_list));
  ASSERT_EQ(tuple_list.size(), 0);
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(k), true, std::string_view(k),
                                                            false, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(k), true, std::string_view(k),
                                                            true, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(0, 1),
                                                            false, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(0, 1),
                                                            true, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(0, 1),
                                                            false, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 0), true, std::string_view(0, 1),
                                                            true, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 1), false, std::string_view(0, 0),
                                                            false, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 1), false, std::string_view(0, 0),
                                                            true, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 1), true, std::string_view(0, 1),
                                                            false, tuple_list));
  ASSERT_EQ(status::ERR_BAD_USAGE, masstree_kvs::scan<char>(std::string_view(0, 1), true, std::string_view(0, 1),
                                                            true, tuple_list));
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(st, test2) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 8;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
  }

  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  constexpr std::size_t v_index = 0;
  for (std::size_t i = 0; i < ary_size; ++i) {
    masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k[i]),
                             false, tuple_list);
    for (std::size_t j = 0; j < i + 1; ++j) {
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v[j].data(), v[j].size()), 0);
    }
  }

  for (std::size_t i = ary_size - 1; i > 1; --i) {
    masstree_kvs::scan<char>(std::string_view(k[i]), false, std::string_view(0, 0), false, tuple_list);
    ASSERT_EQ(tuple_list.size(), ary_size - i);
    for (std::size_t j = i; j < ary_size; ++j) {
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j - i)), v[j].data(), v[j].size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(st, test3) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 15;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
  }
  constexpr std::size_t value_index = 0;
  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::scan(std::string_view(k[i]), false, std::string_view(0, 0), false,
                                             tuple_list));
    ASSERT_EQ(tuple_list.size(), ary_size - i);
    for (std::size_t j = i; j < ary_size; ++j) {
      ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v[j].data(), v[j].size()), 0);
    }
  }
  for (std::size_t i = ary_size - 1; i > 1; --i) {
    ASSERT_EQ(status::OK, masstree_kvs::scan(std::string_view(0, 0), false, std::string_view(k[i]), false, tuple_list));
    ASSERT_EQ(tuple_list.size(), i + 1);
    for (std::size_t j = 0; j < i; ++j) {
      ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j)), v[j].data(), v[j].size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(st, test4) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(1, 'a' + i);
    v[i].assign(1, 'a' + i);
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k[i]}, v[i].data(), v[i].size()));
  }
  constexpr std::size_t value_index = 0;
  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::scan(std::string_view(k[i]), false, std::string_view(0, 0), false,
                                             tuple_list));
    for (std::size_t j = i; j < ary_size; ++j) {
      ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v[j].data(), v[j].size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(st, test5) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::size_t ary_size;
  /**
   * first border split occurs at inserting (base_node::key_slice_length + 1) times.
   * after first border split, split occurs at inserting (base_node::key_slice_length / 2 + 1) times.
   * first interior split occurs at splitting interior_node::child_length times.
   */
  ary_size =
          base_node::key_slice_length + 1 + (base_node::key_slice_length / 2 + 1) * (interior_node::child_length - 1);

  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(1, i);
    v[i].assign(1, i);
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k[i]}, v[i].data(), v[i].size()));
  }
  constexpr std::size_t value_index = 0;
  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::scan(std::string_view(k[i]), false, std::string_view(0, 0), false,
                                             tuple_list));
    for (std::size_t j = i; j < ary_size; ++j) {
      ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v[j].data(), v[j].size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

} // namespace yakushima

