/**
 * @file kvs_test.cpp
 */

#include <future>
#include <thread>
#include <tuple>

#include "gtest/gtest.h"

#include "kvs.h"

#include "include/global_variables_decralation.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class kvs_test : public ::testing::Test {
protected:
  kvs_test() {
    masstree_kvs::init_kvs();
  }

  ~kvs_test() {
    masstree_kvs::destroy();
  }
};

TEST_F(kvs_test, init) {
  ASSERT_EQ(masstree_kvs::init_kvs(), status::OK_ROOT_IS_NULL);
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  ASSERT_NE(base_node::get_root(), nullptr);
  ASSERT_EQ(masstree_kvs::init_kvs(), status::OK_DESTROY_ALL);
  ASSERT_EQ(base_node::get_root(), nullptr);
}

TEST_F(kvs_test, single_put_get_to_one_border) {
  /**
   * put one key-value
   */
  masstree_kvs::init_kvs();
  ASSERT_EQ(base_node::get_root(), nullptr);
  std::string k("a"), v("v-a");
  ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view(k), v.data(), v.size()));
  base_node* root = base_node::get_root(); // this is border node.
  ASSERT_NE(root, nullptr);
  key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
  ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
  ASSERT_EQ(root->get_key_length_at(0), k.size());
  std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k));
  ASSERT_NE(std::get<0>(tuple), nullptr);
  ASSERT_EQ(std::get<1>(tuple), v.size());
  ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
}

TEST_F(kvs_test, multiple_put_get_same_null_char_key_slice_and_different_key_length_to_single_border) {
  /**
   * put one key-value
   */
  masstree_kvs::init_kvs();
  constexpr std::size_t ary_size = 9;
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
  /**
   * there are bug.
   */
  for (std::size_t i = 0; i < ary_size; ++i) {
    constexpr std::size_t value_index = 0, size_index = 1;
    std::tuple<char *, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k[i]));
    ASSERT_EQ(memcmp(std::get<value_index>(tuple), v[i].data(), v[i].size()), 0);
    ASSERT_EQ(std::get<size_index>(tuple), v[i].size());
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
}

TEST_F(kvs_test, multiple_put_get_same_null_char_key_slice_and_different_key_length_to_multiple_border) {
  /**
   * put one key-value
   */
  masstree_kvs::init_kvs();
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
      ASSERT_EQ(br->get_permutation_cnk(), 9);
    }
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    constexpr std::size_t value_index = 0, size_index = 1;
    std::tuple<char*, std::size_t> tuple = masstree_kvs::get<char>(std::string_view(k[i]));
    ASSERT_EQ(std::get<size_index>(tuple), v[i].size());
    ASSERT_EQ(memcmp(std::get<value_index>(tuple), v[i].data(), v[i].size()), 0);
  }
  /**
   * check next layer is border.
   */
  border_node *br = dynamic_cast<border_node *>(base_node::get_root());
  ASSERT_EQ(typeid(*br->get_lv_at(8)->get_next_layer()), typeid(border_node));
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
}

#if 0
TEST_F(kvs_test, put_until_creating_interior_node) {
  masstree_kvs::init_kvs();
  constexpr std::size_t ary_size = base_node::key_slice_length + 1;
  std::string k[ary_size], v[ary_size];
  for (std::size_t i = 0; i < ary_size; ++i) {
    k[i].assign(1, 'a'+i);
    v[i].assign(1, 'a'+i);
  }
  for (std::size_t i = 0; i < ary_size; ++i) {
    ASSERT_EQ(status::OK, masstree_kvs::put(std::string_view{k[i]}, v[i].data(), v[i].size()));
  }
  ASSERT_EQ(masstree_kvs::destroy(), status::OK_DESTROY_ALL);
}
#endif

}  // namespace yakushima::testing
