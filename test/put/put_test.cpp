/**
 * @file put_put_test.cpp
 */

#include <algorithm>
#include <array>
#include <future>
#include <random>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string st{"1"}; // NOLINT

class put_test : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(st);
    }

    void TearDown() override { fin(); }
};

TEST_F(put_test, put_at_non_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::WARN_STORAGE_NOT_EXIST,
              put(token, v, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_at_existing_storage) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(token, st, "", v.data(), v.size()));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_to_root_border_split) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = key_slice_length + 1;
    std::array<std::string, ary_size> k{};
    std::array<std::string, ary_size> v{};
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, 'a' + i); // NOLINT
        v.at(i).assign(1, 'a' + i); // NOLINT
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is interior
    auto* n = ti->load_root_ptr();
    ASSERT_EQ(typeid(*n), typeid(interior_node)); // NOLINT
    auto* in = dynamic_cast<interior_node*>(ti->load_root_ptr());
    // check child is border node which has 8 elements
    auto* bn = dynamic_cast<border_node*>(in->get_child_at(0));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);
    // check that it is not root node
    ASSERT_EQ(bn->get_stable_version().get_root(), false);
    bn = dynamic_cast<border_node*>(in->get_child_at(1));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);
    // check that it is not root node
    ASSERT_EQ(bn->get_stable_version().get_root(), false);

    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_to_root_interior_split_layer0) { // NOLINT
    auto make_key = [](std::size_t i) {
        std::ostringstream ss;
        ss << std::hex << std::setw(6) << std::setfill('0') << i;
        return ss.str();
    };

    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 8 * (key_slice_length + 2);
    std::array<std::string, ary_size> k{};
    std::array<std::string, ary_size> v{};
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(make_key(i)); // NOLINT
        v.at(i).assign(make_key(i)); // NOLINT
    }
    for (std::size_t i = 0; i < ary_size - 1; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is interior with 16 (key_slice_length+1) children
    auto* n = ti->load_root_ptr();
    ASSERT_FALSE(n->get_version_border());
    auto* rn = dynamic_cast<interior_node*>(n);
    ASSERT_EQ(rn->get_n_keys(), key_slice_length);
    // children are all border node
    for (std::size_t i = 0; i <= rn->get_n_keys(); ++i) {
        ASSERT_TRUE(rn->get_child_at(i)->get_version_border()) << i;
    }

    // next put cause splitting
    for (std::size_t i = ary_size - 1; i < ary_size; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is interior with 2 children
    n = ti->load_root_ptr();
    ASSERT_FALSE(n->get_version_border());
    rn = dynamic_cast<interior_node*>(n);
    ASSERT_EQ(rn->get_n_keys(), 1);
    // check child 0 is interior node, not root
    ASSERT_FALSE(rn->get_child_at(0)->get_version_border());
    auto* in0 = dynamic_cast<interior_node*>(rn->get_child_at(0));
    ASSERT_FALSE(in0->get_version_root());
    // check child 1 is interior node, not root
    ASSERT_FALSE(rn->get_child_at(1)->get_version_border());
    auto* in1 = dynamic_cast<interior_node*>(rn->get_child_at(1));
    ASSERT_FALSE(in1->get_version_root());
    // sum of num children is key_slice_length+2
    ASSERT_EQ(in0->get_n_keys() + in1->get_n_keys(), key_slice_length); // (key_slice_length+2-N0)-1 + (N0)-1

    //display();

    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, put_to_root_interior_split_layer1) { // NOLINT
    auto make_key = [](std::size_t i) {
        std::ostringstream ss;
        ss << std::hex << std::setw(14) << std::setfill('0') << i;
        return ss.str();
    };

    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 8 * (key_slice_length + 2);
    std::array<std::string, ary_size> k{};
    std::array<std::string, ary_size> v{};
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(make_key(i)); // NOLINT
        v.at(i).assign(make_key(i)); // NOLINT
    }
    for (std::size_t i = 0; i < ary_size - 1; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is border node with 1 child
    auto* n = ti->load_root_ptr();
    ASSERT_TRUE(n->get_version_border());
    auto* bn = dynamic_cast<border_node*>(n);
    ASSERT_EQ(bn->get_permutation_cnk(), 1);
    // check L1 root is interior with 16 (key_slice_length+1) children
    auto *l1r = bn->get_lv_at(0)->get_next_layer();
    ASSERT_FALSE(l1r->get_version_border());
    auto* rn = dynamic_cast<interior_node*>(l1r);
    ASSERT_EQ(rn->get_n_keys(), key_slice_length);
    // children are all border node
    for (std::size_t i = 0; i <= rn->get_n_keys(); ++i) {
        ASSERT_TRUE(rn->get_child_at(i)->get_version_border()) << i;
    }

    // next put cause splitting
    for (std::size_t i = ary_size - 1; i < ary_size; ++i) {
        ASSERT_EQ(status::OK,
                  put(token, st, k.at(i), v.at(i).data(), v.at(i).size()));
    }

    // check root is border node with 1 child
    n = ti->load_root_ptr();
    ASSERT_TRUE(n->get_version_border());
    bn = dynamic_cast<border_node*>(n);
    ASSERT_EQ(bn->get_permutation_cnk(), 1);
    // check L1 root is interior with 2 children
    l1r = bn->get_lv_at(0)->get_next_layer();
    ASSERT_FALSE(l1r->get_version_border());
    rn = dynamic_cast<interior_node*>(l1r);
    ASSERT_EQ(rn->get_n_keys(), 1);
    // check child 0 is interior node, not root
    ASSERT_FALSE(rn->get_child_at(0)->get_version_border());
    auto* in0 = dynamic_cast<interior_node*>(rn->get_child_at(0));
    ASSERT_FALSE(in0->get_version_root());
    // check child 1 is interior node, not root
    ASSERT_FALSE(rn->get_child_at(1)->get_version_border());
    auto* in1 = dynamic_cast<interior_node*>(rn->get_child_at(1));
    ASSERT_FALSE(in1->get_version_root());
    // sum of num children is key_slice_length+2
    ASSERT_EQ(in0->get_n_keys() + in1->get_n_keys(), key_slice_length); // (key_slice_length+2-N0)-1 + (N0)-1

    //display();

    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    ASSERT_EQ(status::OK, put(token, st, "k", v.data(), v.size()));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key_len9) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string k(9, '0');
    std::string v{"v"};
    yakushima::node_version64* nvp{};
    char* tmp_created_value_ptr{};
    ASSERT_EQ(status::OK,
              put(token, st, k, v.data(), v.size(), &tmp_created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true, &nvp));

    // verify
    auto* n = ti->load_root_ptr();
    ASSERT_EQ(typeid(*n), typeid(border_node)); // NOLINT
    auto* nvp_first_border_node = n->get_version_ptr();
    ASSERT_EQ(nvp, nvp_first_border_node);
    auto* n_second_border_node =
            dynamic_cast<border_node*>(n)->get_lv_at(0)->get_next_layer();
    ASSERT_NE(nvp, n_second_border_node->get_version_ptr());

    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key_twice_unique_rest_true) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    char* created_value_ptr{};
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), true));
    ASSERT_EQ(status::WARN_UNIQUE_RESTRICTION,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), true));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, one_key_twice_unique_rest_false) { // NOLINT
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    char* created_value_ptr{};
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), false));
    ASSERT_EQ(status::OK,
              put(token, st, "k", v.data(), v.size(), &created_value_ptr,
                  static_cast<std::align_val_t>(alignof(char)), false));
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(put_test, inserted_node_info) {
    tree_instance* ti{};
    find_storage(st, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    // test
    std::string v{"v"};
    char* created_value_ptr{};
    inserted_node_info ii1;
    inserted_node_info ii2;
    // insert to empty root
    ASSERT_EQ(status::OK,
              put(token, st, "a", v.data(), v.size(), &created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true, &ii1));
    EXPECT_NE(ii1.modified_nvp, nullptr);
    EXPECT_EQ(ii1.created_nvp, nullptr);
    for (std::size_t i = 1; i < key_slice_length; i++) {
        // insert to border node
        ASSERT_EQ(status::OK,
                  put(token, st, std::to_string(i), v.data(), v.size(), &created_value_ptr,
                      static_cast<value_align_type>(alignof(char)), true, &ii2));
        EXPECT_EQ(ii2.modified_nvp, ii1.modified_nvp);
        EXPECT_EQ(ii2.created_nvp, nullptr);
    }
    // insert to border node (split)
    ASSERT_EQ(status::OK,
              put(token, st, "b", v.data(), v.size(), &created_value_ptr,
                  static_cast<value_align_type>(alignof(char)), true, &ii2));
    EXPECT_EQ(ii2.modified_nvp, ii1.modified_nvp);
    EXPECT_NE(ii2.created_nvp, nullptr);
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
