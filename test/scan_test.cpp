/**
 * @file scan_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

class st : public ::testing::Test {
    void SetUp() override {
        init();
    }

    void TearDown() override {
        fin();
    }
};

TEST_F(st, basic_usage) { // NOLINT
    /**
     * basic usage
     */
    ASSERT_EQ(base_node::get_root_ptr(), nullptr);
    std::string k("k");
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(std::string_view(k), v.data(), v.size()));
    std::vector<std::pair<char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) return false;
        if (tup_lis.size() != nv.size()) return false;
        if (std::get<1>(tup_lis.at(0)) != v.size()) return false;
        if (memcmp(std::get<0>(tup_lis.at(0)), v.data(), v.size()) != 0) return false;
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) return false;
        if (tup_lis.size() != nv.size()) return false;
        return true;
    };
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(true, verify_no_exist());
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(true, verify());
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE, scan<char>("", scan_endpoint::EXCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE, scan<char>("", scan_endpoint::INF, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_against_single_put_null_key_to_one_border) { // NOLINT
    /**
     * put one key-value non-null key
     */
    std::string k;
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(std::string_view(k), v.data(), v.size()));
    std::vector<std::pair<char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify_exist = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) return false;
        if (tup_lis.size() != nv.size()) return false;
        if (std::get<1>(tup_lis.at(0)) != v.size())return false;
        if (memcmp(std::get<0>(tup_lis.at(0)), v.data(), v.size()) != 0)return false;
        return true;
    };
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_against_single_put_non_null_key_to_one_border) { // NOLINT
    /**
     * put one key-value non-null key
     */
    std::string k("k");
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(std::string_view(k), v.data(), v.size()));
    std::vector<std::pair<char*, std::size_t>> tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify_exist = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) return false;
        if (tup_lis.size() != nv.size()) return false;
        if (std::get<1>(tup_lis.at(0)) != v.size()) return false;
        if (memcmp(std::get<0>(tup_lis.at(0)), v.data(), v.size()) != 0) return false;
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) return false;
        if (tup_lis.size() != nv.size()) return false;
        return true;
    };
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INF, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>("", scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INF, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INF, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INF, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::EXCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::EXCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::EXCLUSIVE, "", scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INF, k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INF, k, scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::ERR_BAD_USAGE, scan<char>(k, scan_endpoint::INF, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::OK, scan<char>(k, scan_endpoint::INCLUSIVE, k, scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(k, scan_endpoint::INCLUSIVE, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(k, scan_endpoint::EXCLUSIVE, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(k, scan_endpoint::EXCLUSIVE, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(k, scan_endpoint::EXCLUSIVE, k, scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_multiple_same_null_char_key_1) { // NOLINT
    /**
     * scan against multiple put same null char key whose length is different each other against single border node.
     */
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 8;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK, put(std::string_view(k.at(i)), v.at(i).data(), v.at(i).size()));
    }

    std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
    constexpr std::size_t v_index = 0;
    for (std::size_t i = 0; i < ary_size; ++i) {
        scan<char>("", scan_endpoint::INF, std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, tuple_list);
        for (std::size_t j = 0; j < i + 1; ++j) {
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)), v.at(j).data(), v.at(j).size()), 0);
        }
    }

    for (std::size_t i = ary_size - 1; i > 1; --i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        scan<char>(std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tuple_list, &nv);
        ASSERT_EQ(tuple_list.size(), ary_size - i);
        ASSERT_EQ(tuple_list.size(), nv.size());
        for (std::size_t j = i; j < ary_size; ++j) {
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j - i)), v.at(j).data(), v.at(j).size()), 0);
        }
    }
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_multiple_same_null_char_key_2) { // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 15;
    std::array<std::string, ary_size> k;
    std::array<std::string, ary_size> v;
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK, put(std::string_view(k.at(i)), v.at(i).data(), v.at(i).size()));
    }
    constexpr std::size_t value_index = 0;
    std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK,
                  scan(std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tuple_list, &nv));
        ASSERT_EQ(tuple_list.size(), ary_size - i);
        ASSERT_EQ(tuple_list.size(), nv.size());
        for (std::size_t j = i; j < ary_size; ++j) {
            ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v.at(j).data(), v.at(j).size()), 0);
        }
    }
    for (std::size_t i = ary_size - 1; i > 0; --i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK,
                  scan("", scan_endpoint::INF, std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, tuple_list, &nv));
        ASSERT_EQ(tuple_list.size(), i + 1);
        ASSERT_EQ(tuple_list.size(), nv.size());
        for (std::size_t j = 0; j < i; ++j) {
            ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j)), v.at(j).data(), v.at(j).size()), 0);
        }
    }
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_against_1_interior_some_border) { // NOLINT
    /**
     * scan against the structure which it has interior node as root, and the interior has some border nodes as children.
     */
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = base_node::key_slice_length + 1;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(1, static_cast<char>('a' + i));
        v.at(i).assign(1, static_cast<char>('a' + i));
    }
    for (std::size_t i = 0; i < ary_size; ++i) {
        ASSERT_EQ(status::OK, put(std::string_view{k.at(i)}, v.at(i).data(), v.at(i).size()));
    }
    constexpr std::size_t value_index = 0;
    std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK,
                  scan(std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tuple_list, &nv));
        for (std::size_t j = i; j < ary_size; ++j) {
            ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v.at(j).data(), v.at(j).size()), 0);
        }
    }
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_against_1_interior_2_interior_some_border) { // NOLINT
    /**
     * scan against the structure which it has interior node as root, root has two interior nodes as children,
     * and each of children has some border nodes as children.
     */
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    /**
     * first border split occurs at inserting_deleting (base_node::key_slice_length + 1) times.
     * after first border split, split occurs at inserting_deleting (base_node::key_slice_length / 2 + 1) times.
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
        ASSERT_EQ(status::OK, put(std::string_view{k.at(i)}, v.at(i).data(), v.at(i).size()));
    }
    constexpr std::size_t value_index = 0;
    std::vector<std::pair<char*, std::size_t>> tuple_list{}; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        ASSERT_EQ(status::OK,
                  scan(std::string_view(k.at(i)), scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tuple_list, &nv));
        for (std::size_t j = i; j < ary_size; ++j) {
            ASSERT_EQ(memcmp(std::get<value_index>(tuple_list.at(j - i)), v.at(j).data(), v.at(j).size()), 0);
        }
    }
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_with_prefix_for_2_layers) { // NOLINT
    /**
     * Scan with prefix for 2 layers.
     */
    std::string k("T6\000\200\000\000\n\200\000\000\001", 11); // NOLINT
    std::string end("T6\001", 3); // NOLINT
    std::string v("bbb");  // NOLINT

    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(put(k, v.data(), v.size()), status::OK);
    std::vector<std::pair<char*, std::size_t>> tuple_list{};
    ASSERT_EQ(status::OK, scan("", scan_endpoint::INF, end, scan_endpoint::EXCLUSIVE, tuple_list));
    ASSERT_EQ(tuple_list.size(), 1);
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_with_prefix_for_2_layers_2) { // NOLINT
    std::string r1("T200\x00\x80\x00\x00\xc7\x80\x00\x01\x91\x80\x00\x01\x2d\x80\x00\x00\x01", 21); // NOLINT
    std::string r2("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x92\x80\x00\x01\x2e\x80\x00\x00\x02",
                   21);                 // NOLINT
    std::string be("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93", 13);                 // NOLINT
    std::string r3("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x93\x80\x00\x01\x2f\x80\x00\x00\x03",
                   21);                 // NOLINT
    std::string en("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94", 13); // NOLINT
    std::string r4("T200\x00\x80\x00\x00\xc8\x80\x00\x01\x94\x80\x00\x01\x30\x80\x00\x00\x04",
                   21);                 // NOLINT
    std::string r5("T200\x00\x80\x00\x00\xc9\x80\x00\x01\x95\x80\x00\x01\x31\x80\x00\x00\x05",
                   21);                 // NOLINT
    std::string v("bbb");  // NOLINT
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(put(r1, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(r2, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(r3, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(r4, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(r5, v.data(), v.size()), status::OK);
    std::vector<std::pair<char*, std::size_t>> tuple_list{};
    ASSERT_EQ(status::OK, scan(be, scan_endpoint::INCLUSIVE, en, scan_endpoint::EXCLUSIVE, tuple_list));
    ASSERT_EQ(tuple_list.size(), 1);
    ASSERT_EQ(leave(token), status::OK);
}
} // namespace yakushima

