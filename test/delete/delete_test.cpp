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

std::string test_storage_name{"1"}; // NOLINT

class dt : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(dt, one_border) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 9;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, std::string_view(k.at(i)),
                      v.at(i).data(), v.at(i).size()));
        /**
          * There are 9 key which has the same slice and the different length.
          * key length == 0, same_slice and length is 1, 2, ..., 8.
          */
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
        auto* br = dynamic_cast<border_node*>(ti->load_root_ptr());
        if (i != ary_size - 1) {
            ASSERT_EQ(br->get_permutation_cnk(), ary_size - i - 1);
        }
    }
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, one_border_shuffle) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = 9;
        std::vector<std::tuple<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_tuple(std::string(i, '\0'), std::to_string(i)));
        }

        std::random_device seed_gen;
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);
        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv.at(i)),
                          std::get<1>(kv.at(i)).data(),
                          std::get<1>(kv.at(i)).size()));
        }

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      remove(token, test_storage_name, std::get<0>(kv.at(i))));
            auto* br = dynamic_cast<border_node*>(ti->load_root_ptr());
            if (i != ary_size - 1) {
                ASSERT_EQ(br->get_permutation_cnk(), ary_size - i - 1);
            }
        }
        ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(dt, two_border) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 10;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, std::string_view(k.at(i)),
                      v.at(i).data(), v.at(i).size()));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
        auto* bn = dynamic_cast<border_node*>(ti->load_root_ptr());
        /**
          * here, tree has two layer constituted by two border node.
          */
        if (i != ary_size - 1) {
            ASSERT_EQ(bn->get_permutation_cnk(), 10 - i - 1);
        }
    }
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, two_border_shuffle) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = 10;
        std::vector<std::tuple<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_tuple(std::string(i, '\0'), std::to_string(i)));
        }

        std::random_device seed_gen;
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);
        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv.at(i)),
                          std::get<1>(kv.at(i)).data(),
                          std::get<1>(kv.at(i)).size()));
        }
        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      remove(token, test_storage_name, std::get<0>(kv.at(i))));
        }
        ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(dt, one_interi_two_bor) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = key_slice_length + 1;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, static_cast<char>(i));
        v.at(i).assign(1, static_cast<char>(i));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, put(token, test_storage_name, k.at(i),
                                  v.at(i).data(), v.at(i).size()));
    }

    constexpr std::size_t lb_n{ary_size / 2};
    for (std::size_t i = 0; i < lb_n; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
    }
    ASSERT_EQ(ti->load_root_ptr()->get_version_border(), true);
    for (std::size_t i = lb_n; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
    }
    ASSERT_NE(ti->load_root_ptr(), nullptr);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, one_interi_two_bor_shuffle) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = key_slice_length + 1;
        std::vector<std::tuple<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_tuple(std::string(1, i), std::string(1, i)));
        }
        std::random_device seed_gen;
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv.at(i)),
                          std::get<1>(kv.at(i)).data(),
                          std::get<1>(kv.at(i)).size()));
        }

        std::sort(kv.begin(), kv.end());
        std::size_t lb_n{ary_size / 2 + 1};
        for (std::size_t i = 0; i < lb_n; ++i) {
            ASSERT_EQ(status::OK,
                      remove(token, test_storage_name, std::get<0>(kv.at(i))));
        }
        ASSERT_EQ(ti->load_root_ptr()->get_version_border(), true);
        for (std::size_t i = lb_n; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      remove(token, test_storage_name, std::get<0>(kv.at(i))));
        }
        ASSERT_NE(ti->load_root_ptr(), nullptr);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(dt, two_interi) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    /**
   * first border split occurs at inserting_deleting (key_slice_length + 1) times.
   * after first border split, split occurs at inserting_deleting (key_slice_length / 2 +
   * 1) times. first interior split occurs at splitting interior_node::child_length times.
   */
    constexpr std::size_t ary_size =
            key_slice_length + 1 +
            (key_slice_length / 2 + 1) * (interior_node::child_length - 1);

    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, static_cast<char>(i));
        v.at(i).assign(1, static_cast<char>(i));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, put(token, test_storage_name, k.at(i),
                                  v.at(i).data(), v.at(i).size()));
        if (i == key_slice_length - 1) {
            /**
       * root is full-border.
       */
            auto* n = ti->load_root_ptr();
            ASSERT_EQ(typeid(*n), typeid(border_node)); // NOLINT
        } else if (i == key_slice_length) {
            /**
       * split and insert.
       */
            auto* n = ti->load_root_ptr();
            ASSERT_EQ(typeid(*n), typeid(interior_node)); // NOLINT
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(0))
                              ->get_permutation_cnk(),
                      8);
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(1))
                              ->get_permutation_cnk(),
                      8);
        } else if (i == key_slice_length + (key_slice_length / 2)) {
            /**
       * root is interior, root has 2 children, child[0] of root has 8 keys and child[1]
       * of root has 15 keys.
       */
            ASSERT_EQ(dynamic_cast<interior_node*>(ti->load_root_ptr())
                              ->get_n_keys(),
                      1);
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(0))
                              ->get_permutation_cnk(),
                      8);
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(1))
                              ->get_permutation_cnk(),
                      15);
        } else if (i == key_slice_length + (key_slice_length / 2) + 1) {
            /**
       * root is interior, root has 3 children, child[0-2] of root has 8 keys.
       */
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(0))
                              ->get_permutation_cnk(),
                      8);
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(1))
                              ->get_permutation_cnk(),
                      8);
            ASSERT_EQ(dynamic_cast<border_node*>(
                              dynamic_cast<interior_node*>(ti->load_root_ptr())
                                      ->get_child_at(2))
                              ->get_permutation_cnk(),
                      8);
        } else if ((i > key_slice_length + (key_slice_length / 2) + 1) &&
                   (i < key_slice_length + (key_slice_length / 2 + 1) *
                                                   (key_slice_length - 1)) &&
                   (i - key_slice_length) % ((key_slice_length / 2 + 1)) == 0) {
            /**
       * When it puts (key_slice_length / 2) keys, the root interior node has
       * (i-base_node::key_slice _length) / (key_slice_length / 2);
       */
            ASSERT_EQ(dynamic_cast<interior_node*>(ti->load_root_ptr())
                              ->get_n_keys(),
                      (i - key_slice_length) / (key_slice_length / 2 + 1) + 1);

        } else if (i == key_slice_length + ((key_slice_length / 2 + 1)) *
                                                   (key_slice_length - 1)) {
            ASSERT_EQ(dynamic_cast<interior_node*>(ti->load_root_ptr())
                              ->get_n_keys(),
                      key_slice_length);
        }
    }

    auto* in = dynamic_cast<interior_node*>(ti->load_root_ptr());
    /**
   * root is interior.
   */
    ASSERT_EQ(in->get_version_border(), false);
    auto* child_of_root = dynamic_cast<interior_node*>(in->get_child_at(0));
    /**
   * child of root[0] is interior.
   */
    ASSERT_EQ(child_of_root->get_version_border(), false);
    child_of_root = dynamic_cast<interior_node*>(in->get_child_at(1));
    /**
   * child of root[1] is interior.
   */
    ASSERT_EQ(child_of_root->get_version_border(), false);
    auto* child_child_of_root =
            dynamic_cast<border_node*>(child_of_root->get_child_at(0));
    /**
   * child of child of root[0] is border.
   */
    ASSERT_EQ(child_child_of_root->get_version_border(), true);

    /**
   * deletion phase
   */
    constexpr std::size_t n_in_bn = key_slice_length / 2 + 1;

    ASSERT_EQ(n_in_bn - 1,
              dynamic_cast<interior_node*>(
                      dynamic_cast<interior_node*>(ti->load_root_ptr())
                              ->get_child_at(0))
                      ->get_n_keys());
    for (std::size_t i = 0; i < n_in_bn; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
    }
    ASSERT_EQ(n_in_bn - 2,
              dynamic_cast<interior_node*>(
                      dynamic_cast<interior_node*>(ti->load_root_ptr())
                              ->get_child_at(0))
                      ->get_n_keys());
    constexpr std::size_t to_sb = (n_in_bn - 2) * n_in_bn;
    for (std::size_t i = n_in_bn; i < n_in_bn + to_sb; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
    }
    ASSERT_EQ(1, dynamic_cast<interior_node*>(
                         dynamic_cast<interior_node*>(ti->load_root_ptr()))
                         ->get_n_keys());
    for (std::size_t i = n_in_bn + to_sb; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, remove(token, test_storage_name, k.at(i)));
    }

    ASSERT_NE(ti->load_root_ptr(), nullptr);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(dt, many) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size =
                key_slice_length * interior_node::child_length + 1;

        std::vector<std::tuple<std::string, std::string>> kv; // NOLINT
        kv.reserve(ary_size);
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_tuple(std::string(1, i), std::string(1, i)));
        }

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv.at(i)),
                          std::get<1>(kv.at(i)).data(),
                          std::get<1>(kv.at(i)).size()));
        }

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      remove(token, test_storage_name, std::get<0>(kv.at(i))));
        }

        ASSERT_NE(ti->load_root_ptr(), nullptr);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

} // namespace yakushima::testing
