/**
 * @file delete_test.cpp
 */

#include <algorithm>
#include <array>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class dt : public ::testing::Test {
protected:
  void SetUp() override {
    masstree_kvs::init();
  }

  void TearDown() override {
    masstree_kvs::fin();
  }
};

TEST_F(dt, test1) { // NOLINT
  /**
   * put one key-value
   */
  Token token{};
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::string k("a");
  std::string v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::string_view(k)));
  ASSERT_EQ(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(dt, test2) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 9;
  std::array<std::string, ary_size> k; // NOLINT
  std::array<std::string, ary_size> v; // NOLINT
  for (std::size_t i = 0; i < ary_size; ++i) {
    k.at(i).assign(i, '\0');
    v.at(i) = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k.at(i)), v.at(i).data(), v.at(i).size()));
    /**
     * There are 9 key which has the same slice and the different length.
     * key length == 0, same_slice and length is 1, 2, ..., 8.
     */
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k.at(i)));
    auto *br = dynamic_cast<border_node *>(base_node::get_root());
    if (i != ary_size - 1) {
      ASSERT_EQ(br->get_permutation_cnk(), ary_size - i - 1);
    }
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(dt, test3) { // NOLINT
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token{};
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = 9;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);
    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::put(std::get<0>(kv.at(i)),
                                              std::get<1>(kv.at(i)).data(), std::get<1>(kv.at(i)).size()));
    }

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::get<0>(kv.at(i))));
      auto *br = dynamic_cast<border_node *>(base_node::get_root());
      if (i != ary_size - 1) {
        ASSERT_EQ(br->get_permutation_cnk(), ary_size - i - 1);
      }
    }
    ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(dt, test4) { // NOLINT
  Token token{};
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 10;
  std::array<std::string, ary_size> k; // NOLINT
  std::array<std::string, ary_size> v; // NOLINT
  for (std::size_t i = 0; i < ary_size; ++i) {
    k.at(i).assign(i, '\0');
    v.at(i) = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k.at(i)), v.at(i).data(), v.at(i).size()));
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k.at(i)));
    auto *bn = dynamic_cast<border_node *>(base_node::get_root());
    /**
     * here, tree has two layer constituted by two border node.
     */
    if (i != ary_size - 1) {
      ASSERT_EQ(bn->get_permutation_cnk(), 10 - i - 1);
    }
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(dt, test5) { // NOLINT
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token{};
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = 10;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(i, '\0'), std::to_string(i)));
    }

    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);
    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::put(std::get<0>(kv.at(i)),
                                              std::get<1>(kv.at(i)).data(), std::get<1>(kv.at(i)).size()));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::get<0>(kv.at(i))));
    }
    ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(dt, test6) { // NOLINT
  Token token{};
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::array<std::string, ary_size> k; // NOLINT
  std::array<std::string, ary_size> v; // NOLINT
  for (std::size_t i = 0; i < ary_size; ++i) {
    k.at(i).assign(1, static_cast<char>(i));
    v.at(i).assign(1, static_cast<char>(i));
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k.at(i)}, v.at(i).data(), v.at(i).size()));
  }

  std::size_t lb_n{ary_size / 2};
  for (std::size_t i = 0; i < lb_n; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }
  ASSERT_EQ(base_node::get_root()->get_version_border(), true);
  for (std::size_t i = lb_n; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }
  ASSERT_EQ(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(dt, test7) { // NOLINT
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::vector<std::tuple<std::string, std::string>> kv;
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(1, i), std::string(1, i)));
    }
    std::random_device seed_gen;
    std::mt19937 engine(seed_gen());
    std::shuffle(kv.begin(), kv.end(), engine);

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::put(std::get<0>(kv.at(i)),
                                              std::get<1>(kv.at(i)).data(), std::get<1>(kv.at(i)).size()));
    }

    std::sort(kv.begin(), kv.end());
    std::size_t lb_n{ary_size / 2 + 1};
    for (std::size_t i = 0; i < lb_n; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::get<0>(kv.at(i))));
    }
    ASSERT_EQ(base_node::get_root()->get_version_border(), true);
    for (std::size_t i = lb_n; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::get<0>(kv.at(i))));
    }
    ASSERT_EQ(base_node::get_root(), nullptr);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

TEST_F(dt, test8) { // NOLINT
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  /**
   * first border split occurs at inserting (base_node::key_slice_length + 1) times.
   * after first border split, split occurs at inserting (base_node::key_slice_length / 2 + 1) times.
   * first interior split occurs at splitting interior_node::child_length times.
   */
  constexpr std::size_t ary_size =
          base_node::key_slice_length + 1 + (base_node::key_slice_length / 2 + 1) * (interior_node::child_length - 1);

  std::array<std::string, ary_size> k; // NOLINT
  std::array<std::string, ary_size> v; // NOLINT
  for (std::size_t i = 0; i < ary_size; ++i) {
    k.at(i).assign(1, static_cast<char>(i));
    v.at(i).assign(1, static_cast<char>(i));
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k[i]}, v[i].data(), v[i].size()));
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
       * root is interior, root has 2 children, child[0] of root has 8 keys and child[1] of root has 15 keys.
       */
      ASSERT_EQ(dynamic_cast<interior_node *>(base_node::get_root())->get_n_keys(), 1);
      ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
              0))->get_permutation_cnk(), 8);
      ASSERT_EQ(dynamic_cast<border_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
              1))->get_permutation_cnk(), 15);
    } else if (i == base_node::key_slice_length + (base_node::key_slice_length / 2) + 1) {
      /**
       * root is interior, root has 3 children, child[0-2] of root has 8 keys.
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

  /**
   * deletion phase
   */
  std::size_t n_in_bn;
  n_in_bn = base_node::key_slice_length / 2 + 1;

  ASSERT_EQ(n_in_bn - 1,
            dynamic_cast<interior_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                    0))->get_n_keys());
  for (std::size_t i = 0; i < n_in_bn; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }
  ASSERT_EQ(n_in_bn - 2,
            dynamic_cast<interior_node *>(dynamic_cast<interior_node *>(base_node::get_root())->get_child_at(
                    0))->get_n_keys());
  std::size_t to_sb = (n_in_bn - 2) * n_in_bn;
  for (std::size_t i = n_in_bn; i < n_in_bn + to_sb; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }
  ASSERT_EQ(1, dynamic_cast<interior_node *>(dynamic_cast<interior_node *>(base_node::get_root()))->get_n_keys());
  for (std::size_t i = n_in_bn + to_sb; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }

  ASSERT_EQ(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(dt, test9) { // NOLINT
  masstree_kvs::fin();
  for (std::size_t h = 0; h < 10; ++h) {
    masstree_kvs::init();
    Token token;
    ASSERT_EQ(masstree_kvs::enter(token), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length * interior_node::child_length + 1;

    std::vector<std::tuple<std::string, std::string>> kv;
    kv.reserve(ary_size);
    for (std::size_t i = 0; i < ary_size; ++i) {
      kv.emplace_back(std::make_tuple(std::string(1, i), std::string(1, i)));
    }

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::put(std::get<0>(kv.at(i)),
                                              std::get<1>(kv.at(i)).data(), std::get<1>(kv.at(i)).size()));
    }

    for (std::size_t i = 0; i < ary_size; ++i) {
      ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::get<0>(kv.at(i))));
    }

    ASSERT_EQ(base_node::get_root(), nullptr);
    ASSERT_EQ(masstree_kvs::leave(token), status::OK);
    masstree_kvs::fin();
  }
  masstree_kvs::init();
}

}  // namespace yakushima::testing
