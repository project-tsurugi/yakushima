/**
 * @file put_get_test.cpp
 */

#include <algorithm>
#include <array>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class kt : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(kt, one_key) { // NOLINT
    /**
   * put one key-value
   */
    std::string k("a");
    std::string v("v-a");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    node_version64* nvp{};
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size(),
                              (char**) nullptr, (value_align_type) sizeof(char),
                              true, &nvp));
    ASSERT_EQ(nvp->get_vsplit(), 0);
    ASSERT_EQ(nvp->get_vinsert_delete(), 1);
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    base_node* root = ti->load_root_ptr(); // this is border node.
    ASSERT_NE(root, nullptr);
    key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
    ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
    ASSERT_EQ(root->get_key_length_at(0), k.size());
    std::pair<char*, std::size_t> tuple{};
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tuple));
    ASSERT_NE(std::get<0>(tuple), nullptr);
    ASSERT_EQ(std::get<1>(tuple), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);

    /**
   * put one key-value : one key is initialized by number.
   * Test zero.
   */
    std::uint64_t base_num{0};
    std::string key_buf{};
    key_buf = std::string{reinterpret_cast<char*>(&base_num), // NOLINT
                          sizeof(base_num)};                  // NOLINT
    ASSERT_EQ(put(token, test_storage_name, key_buf, v.data(), v.size()),
              status::OK);
    ASSERT_EQ(status::OK, get<char>(test_storage_name, key_buf, tuple));
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);

    /**
   * put one key-value : one key is initialized by number.
   * Test five.
   */
    base_num = 5;
    key_buf = std::string{reinterpret_cast<char*>(&base_num), // NOLINT
                          sizeof(base_num)};                  // NOLINT
    ASSERT_EQ(put(token, test_storage_name, key_buf, v.data(), v.size()),
              status::OK);
    ASSERT_EQ(status::OK, get<char>(test_storage_name, key_buf, tuple));
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);

    /**
   * put one key-value : one key is initialized by number and padding of 0.
   * Test five.
   */
    std::string padding(56, '0');
    ASSERT_EQ(put(token, test_storage_name, padding + key_buf, v.data(),
                  v.size()),
              status::OK);
    ASSERT_EQ(status::OK,
              get<char>(test_storage_name, padding + key_buf, tuple));
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);

    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, one_key_twice_by_different_value) { // NOLINT
    /**
   * put one key twice
   */
    std::string k{"k"};
    std::string v{"v"};
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    std::pair<char*, std::size_t> tup{};
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tup));
    ASSERT_NE(std::get<0>(tup), nullptr);
    ASSERT_EQ(std::get<1>(tup), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tup), v.data(), v.size()), 0);
    v = "v2";
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tup));
    ASSERT_NE(std::get<0>(tup), nullptr);
    ASSERT_EQ(std::get<1>(tup), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tup), v.data(), v.size()), 0);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, one_key_third_by_different_value) { // NOLINT
    /**
   * put one key twice
   */
    std::string k{"k"};
    std::string v{"v"};
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // put k v
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    std::pair<char*, std::size_t> tup{};
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tup));
    ASSERT_NE(std::get<0>(tup), nullptr);
    ASSERT_EQ(std::get<1>(tup), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tup), v.data(), v.size()), 0);
    v = "v2";
    // put k v2
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tup));
    ASSERT_NE(std::get<0>(tup), nullptr);
    ASSERT_EQ(std::get<1>(tup), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tup), v.data(), v.size()), 0);
    v = "v3";
    // put k v3
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    ASSERT_EQ(status::OK, get<char>(test_storage_name, k, tup));
    ASSERT_NE(std::get<0>(tup), nullptr);
    ASSERT_EQ(std::get<1>(tup), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tup), v.data(), v.size()), 0);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, one_key_long_value) { // NOLINT
    /**
   * put one key-long_value
   */
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    ASSERT_NE(ti->load_root_ptr(), nullptr);
    std::string k("a");
    std::string v(100, 'a');
    ASSERT_EQ(v.size(), 100);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k),
                              v.data(), v.size()));
    base_node* root = ti->load_root_ptr(); // this is border node.
    ASSERT_NE(root, nullptr);
    key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
    ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
    ASSERT_EQ(root->get_key_length_at(0), k.size());
    std::pair<char*, std::size_t> tuple{};
    ASSERT_EQ(status::OK,
              get<char>(test_storage_name, std::string_view(k), tuple));
    ASSERT_NE(std::get<0>(tuple), nullptr);
    ASSERT_EQ(std::get<1>(tuple), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
