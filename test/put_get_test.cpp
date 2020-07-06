/**
 * @file kvs_test.cpp
 */

#include <algorithm>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class kvs_test : public ::testing::Test {
protected:
  kvs_test() {
    masstree_kvs::init();
  }

  ~kvs_test() {
    masstree_kvs::fin();
  }
};

TEST_F(kvs_test, single_put_get_to_one_border) {
  /**
   * put one key-value
   */
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  base_node *root = base_node::get_root(); // this is border node.
  ASSERT_NE(root, nullptr);
  key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
  ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
  ASSERT_EQ(root->get_key_length_at(0), k.size());
  std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k));
  ASSERT_NE(std::get<0>(tuple), nullptr);
  ASSERT_EQ(std::get<1>(tuple), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(kvs_test,
       multiple_put_get_same_null_char_key_slice_and_different_key_length_to_single_border) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 8;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
    border_node *br = dynamic_cast<border_node *>(base_node::get_root());
    /**
     * There are 9 key which has the same slice and the different length.
     * key length == 0, same_slice and length is 1, 2, ..., 8.
     */
    ASSERT_EQ(br->get_permutation_cnk(), i + 1);
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    constexpr std::size_t value_index = 0, size_index = 1;
    std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k[i]));
    ASSERT_EQ(memcmp(std::get<value_index>(tuple), v[i].data(), v[i].size()), 0);
    ASSERT_EQ(std::get<size_index>(tuple), v[i].size());
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(kvs_test,
       multiple_put_get_same_null_char_key_slice_and_different_key_length_to_single_border_with_shuffle) {
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = 8;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);
    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK,
                masstree_kvs::put(std::get<0>(kv[i]), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
      constexpr std::size_t value_index = 0, size_index = 1;
      std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::get<0>(kv[i]));
      ASSERT_EQ(std::get<size_index>(tuple), std::get<1>(kv[i]).size());
      ASSERT_EQ(memcmp(std::get<value_index>(tuple), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()), 0);
    }

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(i, '\0');
      ASSERT_EQ(status::OK,
                masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false, tuple_list));
      ASSERT_EQ(tuple_list.size(), i + 1);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }

    for (std::size_t i = ary_size - 1; i < 1; --i) {
      std::string k(i, '\0');
      ASSERT_EQ(status::OK,
                masstree_kvs::scan<char>(std::string_view(k), false, std::string_view(0, 0), false, tuple_list));
      ASSERT_EQ(tuple_list.size(), ary_size - i);
      for (std::size_t j = i; j < ary_size; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }

    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(kvs_test,
       multiple_put_get_same_null_char_key_slice_and_different_key_length_to_multiple_border) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 15;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
    border_node *br = dynamic_cast<border_node *>(base_node::get_root());
    if (i <= 8) {
      /**
       * There are 9 key which has the same slice and the different length.
       * key length == 0, same_slice and length is 1, 2, ..., 8.
       */
      ASSERT_EQ(br->get_permutation_cnk(), i + 1);
    } else {
      /**
       * The key whose the length of same parts is more than 8, it should be next_layer.
       * So the number of keys should not be change.
       */
      ASSERT_EQ(br->get_permutation_cnk(), 10);
    }
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    constexpr std::size_t value_index = 0, size_index = 1;
    std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k[i]));
    ASSERT_EQ(std::get<size_index>(tuple), v[i].size());
    ASSERT_EQ(memcmp(std::get<value_index>(tuple), v[i].data(), v[i].size()), 0);
  }
  /**
   * check next layer is border.
   */
  border_node *br = dynamic_cast<border_node *>(base_node::get_root());
  ASSERT_EQ(typeid(*(br->get_lv_at(9)->get_next_layer())), typeid(border_node));
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(kvs_test,
       multiple_put_get_same_null_char_key_slice_and_different_key_length_to_multiple_border_with_shuffle) {
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = 15;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(i, 'a'), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK,
                masstree_kvs::put(std::get<0>(kv[i]), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
      constexpr std::size_t value_index = 0, size_index = 1;
      std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::get<0>(kv[i]));
      ASSERT_EQ(std::get<size_index>(tuple), std::get<1>(kv[i]).size());
      ASSERT_EQ(memcmp(std::get<value_index>(tuple), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()), 0);
    }

    std::vector<std::tuple<char *, std::size_t>> tuple_list;
    for (std::size_t i = 1; i < ary_size; ++i) {
      std::string k(i, 'a');
      ASSERT_EQ(status::OK,
                masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(k), false, tuple_list));
      ASSERT_EQ(tuple_list.size(), i + 1);
      for (std::size_t j = 0; j < i + 1; ++j) {
        std::string v(std::to_string(j));
        ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(j)), v.data(), v.size()), 0);
      }
    }
    ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(kvs_test, put_until_creating_interior_node) {
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
  interior_node *in = dynamic_cast<interior_node *>(base_node::get_root());
  ASSERT_EQ(typeid(*base_node::get_root()), typeid(interior_node));
  border_node *bn = dynamic_cast<border_node *>(in->get_child_at(0));
  ASSERT_EQ(bn->get_permutation_cnk(), 8);
  bn = dynamic_cast<border_node *>(in->get_child_at(1));
  ASSERT_EQ(bn->get_permutation_cnk(), 8);

  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(kvs_test,
       put_until_creating_interior_node_with_shuffle) {
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(1, 'a' + i), std::string(1, 'a' + i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK,
                masstree_kvs::put(std::get<0>(kv[i]), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()));
    }
    ASSERT_EQ(typeid(*base_node::get_root()), typeid(interior_node));
    ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(kvs_test,
       put_until_first_split_of_interior_node) {
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
            base_node::key_slice_length + 1 +
            (base_node::key_slice_length / 2 + 1) * (interior_node::child_length - 1);
  } else {
    ary_size =
            base_node::key_slice_length + 1 + (base_node::key_slice_length / 2) * (interior_node::child_length - 1);
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

TEST_F(kvs_test,
       put_until_first_split_of_interior_node_with_shuffle) {
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 30; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    std::size_t ary_size = base_node::key_slice_length * interior_node::child_length + 1;

    std::vector<std::tuple<std::string, std::string>> kv;
    kv.reserve(ary_size);
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(1, i), std::string(1, i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);

    std::size_t putctr = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK,
                masstree_kvs::put(std::get<0>(kv[i]), std::get<1>(kv[i]).data(), std::get<1>(kv[i]).size()));
      if (i > base_node::key_slice_length / 2 * interior_node::child_length) { // about minimum
        base_node *bn = base_node::get_root();
        if (!bn->get_version_border()) {
          interior_node *in = dynamic_cast<interior_node *>(bn);
          if (in->get_n_keys() == 2) {
            base_node *child = in->get_child_at(0);
            if (!child->get_version_border()) {
              putctr = i;
              break;
            }
          }
        }
      }
    }

    std::sort(kv.begin(), kv.end());
    for (std::size_t i = 0; i <= putctr; ++i) {
      std::vector<std::tuple<char *, std::size_t>> tuple_list;
      masstree_kvs::scan<char>(std::string_view(0, 0), false, std::string_view(std::get<0>(kv[i])), false, tuple_list);
      if (tuple_list.size() != i + 1) {
        ASSERT_EQ(tuple_list.size(), i + 1);
      }
      for (std::size_t j = 0; j < i + 1; ++j) {
        ASSERT_EQ(memcmp(std::get<0>(tuple_list.at(j)), std::get<1>(kv[i]).data(), std::get<1>(tuple_list.at(j))), 0);
      }
    }

    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

}  // namespace yakushima::testing
