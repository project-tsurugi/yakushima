/**
 * @file multi_thread_put_test.cpp
 */

#include <algorithm>
#include <future>
#include <random>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

#include "debug.hh"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class multi_thread_put_test : public ::testing::Test {
protected:
  multi_thread_put_test() {
    masstree_kvs::init();
  }

  ~multi_thread_put_test() {
    masstree_kvs::fin();
  }
};

TEST_F(multi_thread_put_test, put_same_null_char_key_slices_and_different_key_length_to_single_border_by_multi_thread) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 9;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 5; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 5; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
      }
    }
  };

  std::thread t(S::work, std::ref(kv2));
  S::work(std::ref(kv1));
  t.join();

  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  constexpr std::size_t v_index = 0;
  for (std::size_t i = 0; i < ary_size; ++i) {
    std::string k(i, '\0');
    masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                             tuple_list);
    for (std::size_t j = 0; j < i + 1; ++j) {
      std::string v(std::to_string(j));
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(multi_thread_put_test,
       put_same_null_char_key_slices_and_different_key_length_to_multiple_border_by_multi_thread) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 15;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < 7; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = 7; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
      }
    }
  };

  std::thread t(S::work, std::ref(kv2));
  S::work(std::ref(kv1));
  t.join();

  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  constexpr std::size_t v_index = 0;
  for (std::size_t i = 0; i < ary_size; ++i) {
    std::string k(i, '\0');
    masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                             tuple_list);
    for (std::size_t j = 0; j < i + 1; ++j) {
      std::string v(std::to_string(j));
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(multi_thread_put_test, put_until_creating_interior_node) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::vector<std::tuple<std::string, std::string>> kv1;
  std::vector<std::tuple<std::string, std::string>> kv2;
  for (std::size_t i = 0; i < ary_size / 2; ++i) {
    kv1.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  for (std::size_t i = ary_size / 2; i < ary_size; ++i) {
    kv2.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
  }
  std::random_device seed_gen;
  std::mt19937 engine(seed_gen());
  std::shuffle(kv1.begin(), kv1.end(), engine);
  std::shuffle(kv2.begin(), kv2.end(), engine);

  struct S {
    static void work(std::vector<std::tuple<std::string, std::string>> &kv) {
      for (auto &i : kv) {
        std::string k(std::get<0>(i)), v(std::get<1>(i));
        ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
      }
    }
  };

  std::thread t(S::work, std::ref(kv2));
  S::work(std::ref(kv1));
  t.join();

  std::vector<std::tuple<char *, std::size_t>> tuple_list;
  constexpr std::size_t v_index = 0;
  for (std::size_t i = 0; i < ary_size; ++i) {
    std::string k(i, '\0');
    masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false,
                             tuple_list);
    for (std::size_t j = 0; j < i + 1; ++j) {
      std::string v(std::to_string(j));
      ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.data(), v.size()), 0);
    }
  }
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

#if 0
TEST_F(multi_thread_put_test, put_until_first_split_of_interior_node) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::size_t ary_size;
  if (base_node::key_slice_length % 2) {
    /**
     * first border split occurs at inserting (base_node::key_slice_length + 1) times.
     * after first border split, split occurs at inserting (base_node::key_slice_length / 2 + 1) times.
     * first interior split occurs at splitting interior_node::child_length times.
     */
    ary_size =
            base_node::key_slice_length + 1 + (base_node::key_slice_length / 2 + 1) * (interior_node::child_length - 1);
  } else {
    ary_size = base_node::key_slice_length + 1 + (base_node::key_slice_length / 2) * (interior_node::child_length - 1);
  }

  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(1, i);
    v[i].assign(1, i);
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k[i]}, v[i].data(), v[i].size()));
    if (base_node::key_slice_length % 2) {
      if (i == base_node::key_slice_length - 1) {
        /**
         * root is full-border.
         */
        ASSERT_EQ(typeid(*base_node::get_root()), typeid(border_node));
      } else if (i == base_node::key_slice_length) {
        /**
         * split and insert.
         */
        ASSERT_EQ(typeid(*base_node::get_root()), typeid(interior_node));
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                0))->get_permutation_cnk(), 8);
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                1))->get_permutation_cnk(), 8);
      } else if (i == base_node::key_slice_length + (base_node::key_slice_length / 2)) {
        /**
         * root is interior, root has 2 childs, child[0] of root has 8 keys and child[1] of root has 15 keys.
         */
        ASSERT_EQ(dynamic_cast<interior_node *>(base_node::get_root())->get_n_keys(), 1);
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                0))->get_permutation_cnk(), 8);
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                1))->get_permutation_cnk(), 15);
      } else if (i == base_node::key_slice_length + (base_node::key_slice_length / 2) + 1) {
        /**
         * root is interior, root has 3 childs, child[0-2] of root has 8 keys.
         */
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                0))->get_permutation_cnk(), 8);
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                1))->get_permutation_cnk(), 8);
        ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                2))->get_permutation_cnk(), 8);
      } else if ((i > base_node::key_slice_length + (base_node::key_slice_length / 2) + 1) &&
                 (i < base_node::key_slice_length +
                      (base_node::key_slice_length / 2 + 1) * (base_node::key_slice_length - 1)) &&
                 (i - base_node::key_slice_length) % ((base_node::key_slice_length / 2 + 1)) == 0) {
        /**
         * When it puts (base_node::key_slice_length / 2) keys, the root interior node has (i-base_node::key_slice
         * _length) / (base_node::key_slice_length / 2);
         */
        ASSERT_EQ(dynamic_cast<interior_node *>(base_node::get_root())->get_n_keys(),
                  (i - base_node::key_slice_length) / (base_node::key_slice_length / 2 + 1) + 1);

      } else if (i == base_node::key_slice_length +
                      ((base_node::key_slice_length / 2 + 1)) * (base_node::key_slice_length - 1)) {
        ASSERT_EQ(dynamic_cast<interior_node *>(base_node::get_root())->get_n_keys(), base_node::key_slice_length);
      }
    }
  }

  interior_node *in = dynamic_cast<interior_node *>(base_node::get_root());
  /**
   * root is interior.
   */
  ASSERT_EQ(in->get_version_border(), false);
  interior_node *child_of_root = dynamic_cast<interior_node *>(in->get_child_at(0));
  /**
   * child of root[0] is interior.
   */
  ASSERT_EQ(child_of_root->get_version_border(), false);
  child_of_root = dynamic_cast<interior_node *>(in->get_child_at(1));
  /**
   * child of root[1] is interior.
   */
  ASSERT_EQ(child_of_root->get_version_border(), false);
  border_node *child_child_of_root = dynamic_cast<border_node *>(child_of_root->get_child_at(0));
  /**
   * child of child of root[0] is border.
   */
  ASSERT_EQ(child_child_of_root->get_version_border(), true);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

#endif
}  // namespace yakushima::testing
