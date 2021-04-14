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

    void TearDown() override {
        fin();
    }
};

TEST_F(kt, short_key) { // NOLINT
    /**
     * put one key-value
     */
    std::string k("a");
    std::string v("v-a");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    node_version64* nvp{};
    ASSERT_EQ(status::OK, put(test_storage_name, k, v.data(), v.size(), (char**) nullptr, (value_align_type) sizeof(char), &nvp));
    ASSERT_EQ(nvp->get_vsplit(), 0);
    ASSERT_EQ(nvp->get_vinsert_delete(), 1);
    tree_instance* ti;
    find_storage(test_storage_name, &ti);
    base_node* root = ti->load_root_ptr(); // this is border node.
    ASSERT_NE(root, nullptr);
    key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
    ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
    ASSERT_EQ(root->get_key_length_at(0), k.size());
    std::pair<char*, std::size_t> tuple = get<char>(test_storage_name, k);
    ASSERT_NE(std::get<0>(tuple), nullptr);
    ASSERT_EQ(std::get<1>(tuple), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, short_key_long_value) { // NOLINT
    /**
     * put one key-long_value
     */
    tree_instance* ti;
    find_storage(test_storage_name, &ti);
    ASSERT_EQ(ti->load_root_ptr(), nullptr);
    std::string k("a");
    std::string v(100, 'a');
    ASSERT_EQ(v.size(), 100);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(test_storage_name, std::string_view(k), v.data(), v.size()));
    base_node* root = ti->load_root_ptr(); // this is border node.
    ASSERT_NE(root, nullptr);
    key_slice_type lvalue_key_slice = root->get_key_slice_at(0);
    ASSERT_EQ(memcmp(&lvalue_key_slice, k.data(), k.size()), 0);
    ASSERT_EQ(root->get_key_length_at(0), k.size());
    std::pair<char*, std::size_t> tuple = get<char>(test_storage_name, std::string_view(k));
    ASSERT_NE(std::get<0>(tuple), nullptr);
    ASSERT_EQ(std::get<1>(tuple), v.size());
    ASSERT_EQ(memcmp(std::get<0>(tuple), v.data(), v.size()), 0);
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

}  // namespace yakushima::testing
