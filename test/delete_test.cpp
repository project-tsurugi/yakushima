/**
 * @file delete_test.cpp
 */

#include <future>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

#include "debug.hh"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class delete_test : public ::testing::Test {
protected:
  delete_test() {
    masstree_kvs::init();
  }

  ~delete_test() {
    masstree_kvs::fin();
  }
};

TEST_F(delete_test, delete_against_single_put_to_one_border) {
  /**
   * put one key-value
   */
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  std::string k("a"), v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  ASSERT_EQ(status::OK, masstree_kvs::remove(token, std::string_view(k)));
  ASSERT_EQ(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(delete_test, delte_against_multiple_put_same_null_char_key_slice_and_different_key_length_to_single_border) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 9;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
    /**
     * There are 9 key which has the same slice and the different length.
     * key length == 0, same_slice and length is 1, 2, ..., 8.
     */
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
    border_node *br = dynamic_cast<border_node *>(base_node::get_root());
    if (i != ary_size - 1) {
      ASSERT_EQ(br->get_permutation_cnk(), ary_size - i - 1);
    }
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(delete_test, delete_against_multiple_put_same_null_char_key_slice_and_different_key_length_to_multiple_border) {
  Token token;
  ASSERT_EQ(masstree_kvs::enter(token), status::OK);
  constexpr std::size_t ary_size = 10;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(i, '\0');
    v[i] = std::to_string(i);
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k[i]), v[i].data(), v[i].size()));
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
    border_node *bn = dynamic_cast<border_node *>(base_node::get_root());
    if (i < 9) {
      /**
       * here, tree has two layer constituted by two border node.
       */
      ASSERT_EQ(bn->get_permutation_cnk(), 10 - i - 1);
    } else if (9 < i && i < ary_size - 1) {
      /**
       * here, tree has two layer constituted by two border node.
       * and the root border has one lv pointing to next_layer.
       */
      ASSERT_EQ(bn->get_permutation_cnk(), 1);
      ASSERT_EQ(dynamic_cast<border_node *>(bn->get_lv_at(0)->get_next_layer())->get_permutation_cnk(),
                ary_size - i - 1);
    }
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

TEST_F(delete_test, delete_against_all_after_put_until_creating_interior_node) {
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

TEST_F(delete_test, DISABLED_delete_against_put_until_first_split_of_interior_node) {
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

  /**
   * deletion phase
   */
  std::size_t n_in_bn;
  if (base_node::key_slice_length % 2) {
    n_in_bn = base_node::key_slice_length / 2 + 1;
  } else {
    /**
     * Future work is a method that can flexibly control node fanout.
     */
    std::cerr << __FILE__ << " : " << __LINE__ << std::endl;
    std::abort();
  }
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
    cout << i << endl;
    ASSERT_EQ(status::OK, masstree_kvs::remove(token, k[i]));
  }

  ASSERT_EQ(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::leave(token), status::OK);
}

}  // namespace yakushima::testing
