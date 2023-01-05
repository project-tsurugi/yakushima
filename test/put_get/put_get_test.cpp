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

TEST_F(kt, test3) { // NOLINT
    /**
     * one border node. same key char, different key length 0-8
     */
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 8;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        node_version64* nvp{};
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, std::string_view(k.at(i)),
                      v.at(i).data(), v.at(i).size(), (char**) nullptr,
                      (value_align_type) sizeof(char), true, &nvp));
        ASSERT_EQ(nvp->get_vinsert_delete(), i + 1);
        auto* br = dynamic_cast<border_node*>(ti->load_root_ptr());
        /**
          * There are 9 key which has the same slice and the different length.
          * key length == 0, same_slice and length is 1, 2, ..., 8.
          */
        ASSERT_EQ(br->get_permutation_cnk(), i + 1);
    }
    constexpr std::size_t value_index = 0;
    constexpr std::size_t size_index = 1;
    for (std::size_t i = 0; i < ary_size; ++i) {
        std::pair<char*, std::size_t> tuple{};
        ASSERT_EQ(status::OK, get<char>(test_storage_name,
                                        std::string_view(k.at(i)), tuple));
        ASSERT_EQ(memcmp(std::get<value_index>(tuple), v.at(i).data(),
                         v.at(i).size()),
                  0);
        ASSERT_EQ(std::get<size_index>(tuple), v.at(i).size());
    }
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, test4) { // NOLINT
    /**
     * test3 random order.
     */
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = 8;
        std::vector<std::pair<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_pair(std::string(i, '\0'), std::to_string(i)));
        }

        std::random_device seed_gen{};
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);
        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv[i]),
                          std::get<1>(kv[i]).data(),
                          std::get<1>(kv[i]).size()));
        }
        for (std::size_t i = 0; i < ary_size; ++i) {
            constexpr std::size_t value_index = 0;
            constexpr std::size_t size_index = 1;
            std::pair<char*, std::size_t> tuple{};
            ASSERT_EQ(status::OK,
                      get<char>(test_storage_name, std::get<0>(kv[i]), tuple));
            ASSERT_EQ(std::get<size_index>(tuple), std::get<1>(kv[i]).size());
            ASSERT_EQ(memcmp(std::get<value_index>(tuple),
                             std::get<1>(kv[i]).data(),
                             std::get<1>(kv[i]).size()),
                      0);
        }

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        for (std::size_t i = 1; i < ary_size; ++i) {
            std::string k(i, '\0');
            ASSERT_EQ(status::OK,
                      scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                                 scan_endpoint::INCLUSIVE, tuple_list));
            ASSERT_EQ(tuple_list.size(), i + 1);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<1>(tuple_list.at(j)), v.data(),
                                 v.size()),
                          0);
            }
        }

        for (std::size_t i = ary_size - 1; i < 1; --i) {
            std::string k(i, '\0');
            ASSERT_EQ(status::OK,
                      scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE,
                                 "", scan_endpoint::INF, tuple_list));
            ASSERT_EQ(tuple_list.size(), ary_size - i);
            for (std::size_t j = i; j < ary_size; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<1>(tuple_list.at(j)), v.data(),
                                 v.size()),
                          0);
            }
        }

        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(kt, test5) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 15;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK, put(token, test_storage_name, k.at(i),
                                  v.at(i).data(), v.at(i).size()));
        auto* br = dynamic_cast<border_node*>(ti->load_root_ptr());
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
        constexpr std::size_t value_index = 0;
        constexpr std::size_t size_index = 1;
        std::pair<char*, std::size_t> tuple{};
        ASSERT_EQ(status::OK, get<char>(test_storage_name,
                                        std::string_view(k.at(i)), tuple));
        ASSERT_EQ(std::get<size_index>(tuple), v.at(i).size());
        ASSERT_EQ(memcmp(std::get<value_index>(tuple), v.at(i).data(),
                         v.at(i).size()),
                  0);
    }
    /**
      * check next layer is border.
      */
    auto* br = dynamic_cast<border_node*>(ti->load_root_ptr());
    auto* n = br->get_lv_at(9)->get_next_layer();
    ASSERT_EQ(typeid(*n), typeid(border_node)); // NOLINT
    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, test6) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = 15;
        std::vector<std::pair<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_pair(std::string(i, 'a'), std::to_string(i)));
        }

        std::random_device seed_gen{};
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv[i]),
                          std::get<1>(kv[i]).data(),
                          std::get<1>(kv[i]).size()));
        }
        for (std::size_t i = 0; i < ary_size; ++i) {
            constexpr std::size_t value_index = 0;
            constexpr std::size_t size_index = 1;
            std::pair<char*, std::size_t> tuple{};
            ASSERT_EQ(status::OK,
                      get<char>(test_storage_name, std::get<0>(kv[i]), tuple));
            ASSERT_EQ(std::get<size_index>(tuple), std::get<1>(kv[i]).size());
            ASSERT_EQ(memcmp(std::get<value_index>(tuple),
                             std::get<1>(kv[i]).data(),
                             std::get<1>(kv[i]).size()),
                      0);
        }

        std::vector<std::tuple<std::string, char*, std::size_t>>
                tuple_list; // NOLINT
        for (std::size_t i = 1; i < ary_size; ++i) {
            std::string k(i, 'a');
            ASSERT_EQ(status::OK,
                      scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                                 scan_endpoint::INCLUSIVE, tuple_list));
            ASSERT_EQ(tuple_list.size(), i + 1);
            for (std::size_t j = 0; j < i + 1; ++j) {
                std::string v(std::to_string(j));
                ASSERT_EQ(memcmp(std::get<1>(tuple_list.at(j)), v.data(),
                                 v.size()),
                          0);
            }
        }
        ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(kt, test7) { // NOLINT
    tree_instance* ti{};
    find_storage(test_storage_name, &ti);
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = key_slice_length + 1;
    std::array<std::string, ary_size> k{};
    std::array<std::string, ary_size> v{};
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, 'a' + i);
        v.at(i).assign(1, 'a' + i);
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, put(token, test_storage_name, k.at(i),
                                  v.at(i).data(), v.at(i).size()));
    }
    auto* in = dynamic_cast<interior_node*>(ti->load_root_ptr());
    auto* n = ti->load_root_ptr();
    ASSERT_EQ(typeid(*n), typeid(interior_node)); // NOLINT
    auto* bn = dynamic_cast<border_node*>(in->get_child_at(0));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);
    bn = dynamic_cast<border_node*>(in->get_child_at(1));
    ASSERT_EQ(bn->get_permutation_cnk(), 8);

    ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, test8) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        constexpr std::size_t ary_size = key_slice_length + 1;
        std::vector<std::pair<std::string, std::string>> kv; // NOLINT
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(std::make_pair(std::string(1, 'a' + i),
                                           std::string(1, 'a' + i)));
        }
        std::random_device seed_gen{};
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);

        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv[i]),
                          std::get<1>(kv[i]).data(),
                          std::get<1>(kv[i]).size()));
        }
        auto* n = ti->load_root_ptr();
        ASSERT_EQ(typeid(*n), typeid(interior_node)); // NOLINT
        ASSERT_EQ(destroy(), status::OK_DESTROY_ALL);
        ASSERT_EQ(leave(token), status::OK);
        destroy();
    }
}

TEST_F(kt, test9) { // NOLINT
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
        k.at(i).assign(1, i);
        v.at(i).assign(1, i);
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
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(kt, test10) { // NOLINT
    for (std::size_t h = 0; h < 1; ++h) {
        create_storage(test_storage_name);
        tree_instance* ti{};
        find_storage(test_storage_name, &ti);
        Token token{};
        ASSERT_EQ(enter(token), status::OK);
        std::size_t ary_size =
                key_slice_length * interior_node::child_length + 1;

        std::vector<std::pair<std::string, std::string>> kv; // NOLINT
        kv.reserve(ary_size);
        for (std::size_t i = 0; i < ary_size; ++i) {
            kv.emplace_back(
                    std::make_pair(std::string(1, i), std::string(1, i)));
        }
        std::random_device seed_gen;
        std::mt19937 engine(seed_gen());
        std::shuffle(kv.begin(), kv.end(), engine);

        std::size_t putctr = 0;
        for (std::size_t i = 0; i < ary_size; ++i) {
            ASSERT_EQ(status::OK,
                      put(token, test_storage_name, std::get<0>(kv[i]),
                          std::get<1>(kv[i]).data(),
                          std::get<1>(kv[i]).size()));
            if (i > key_slice_length / 2 *
                            interior_node::child_length) { // about minimum
                base_node* bn = ti->load_root_ptr();
                if (!bn->get_version_border()) {
                    auto* in = dynamic_cast<interior_node*>(bn);
                    if (in->get_n_keys() == 2) {
                        base_node* child = in->get_child_at(0);
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
            std::vector<std::tuple<std::string, char*, std::size_t>>
                    tuple_list; // NOLINT
            scan<char>(test_storage_name, "", scan_endpoint::INF,
                       std::get<0>(kv[i]), scan_endpoint::INCLUSIVE,
                       tuple_list);
            if (tuple_list.size() != i + 1) {
                ASSERT_EQ(tuple_list.size(), i + 1);
            }
            for (std::size_t j = 0; j < i + 1; ++j) {
                ASSERT_EQ(memcmp(std::get<1>(tuple_list.at(j)),
                                 std::get<1>(kv[i]).data(),
                                 std::get<2>(tuple_list.at(j))),
                          0);
            }
        }
        destroy();
    }
}

TEST_F(kt, test11) { // NOLINT
    /**
   * test about argument @a created_ptr of put function.
   */
    Token token{};
    ASSERT_EQ(status::OK, enter(token));
    std::string k("a");
    std::string v("b");
    char* created_ptr{};
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size(),
                              &created_ptr));
    ASSERT_EQ(memcmp(created_ptr, v.data(), v.size()), 0);
}

TEST_F(kt, test12) { // NOLINT
    Token token{};
    std::string k("a");    // NOLINT
    std::string k2("aa");  // NOLINT
    std::string k3("aac"); // NOLINT
    std::string k4("b");   // NOLINT
    std::string v("v");    // NOLINT
    ASSERT_EQ(status::OK, enter(token));
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size()));
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, k2, v.data(), v.size()));
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, k3, v.data(), v.size()));
    ASSERT_EQ(status::OK,
              put(token, test_storage_name, k4, v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list;
    scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, k4,
               scan_endpoint::EXCLUSIVE, tuple_list);
    ASSERT_EQ(tuple_list.size(), 2);
}
} // namespace yakushima::testing
