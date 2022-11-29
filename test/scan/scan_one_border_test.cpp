/**
 * @file scan_test.cpp
 */

#include <array>

#include "gtest/gtest.h"

#include "kvs.h"

using namespace yakushima;

namespace yakushima::testing {

std::string test_storage_name{"1"}; // NOLINT

class st : public ::testing::Test {
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(st, scan_against_single_put_null_key_to_one_border) { // NOLINT
    /**
      * put one key-value null key
      */
    std::string k;
    std::string v("v");
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k),
                              v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify_exist = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) return false;
        if (tup_lis.size() != nv.size()) return false;
        if (std::get<2>(tup_lis.at(0)) != v.size()) return false;
        if (memcmp(std::get<1>(tup_lis.at(0)), v.data(), v.size()) != 0) {
            return false;
        }
        return true;
    };
    // inf inf
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF,
                                     "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // inc inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // inf inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // inc inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
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
    ASSERT_EQ(status::OK, put(token, test_storage_name, std::string_view(k),
                              v.data(), v.size()));
    std::vector<std::tuple<std::string, char*, std::size_t>>
            tup_lis{}; // NOLINT
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    auto verify_exist = [&tup_lis, &nv, &v]() {
        if (tup_lis.size() != 1) { return false; }
        if (tup_lis.size() != nv.size()) { return false; }
        if (std::get<2>(tup_lis.at(0)) != v.size()) { return false; }
        if (memcmp(std::get<1>(tup_lis.at(0)), v.data(), v.size()) != 0) {
            return false;
        }
        return true;
    };
    auto verify_no_exist = [&tup_lis, &nv]() {
        if (!tup_lis.empty()) { return false; }
        if (nv.size() != 1) { return false; }
        return true;
    };
    // no endpoint string information
    // inf inf
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF,
                                     "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // inc inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // inf inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // inc inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // use endpoint string information
    // "" inf, k inf
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, "", scan_endpoint::INF,
                                     k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "" inf, k inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "" inf, k exc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INF, k,
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "" inc, k inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "" inc, k inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "" inc, k exc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, "", scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" inf, "" exc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INF, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" inf, "" inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INF, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" inf, "" inf
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, k, scan_endpoint::INF,
                                     "", scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inc, "" inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inc, "" inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" inc, "" exc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" exc, "" inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" exc, "" inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" exc, "" exc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, "",
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_no_exist(), true);
    // "k" inf, "k" exc
    ASSERT_EQ(status::OK, scan<char>(test_storage_name, k, scan_endpoint::INF,
                                     k, scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inf, "k" inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INF, k,
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inf, "k" exc
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, k, scan_endpoint::INF, k,
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    // "k" inf, "k" inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inc, "k" inc
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" inc, "k" exc
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    // "k" inc, "k" inf
    ASSERT_EQ(status::OK,
              scan<char>(test_storage_name, k, scan_endpoint::INCLUSIVE, k,
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(verify_exist(), true);
    // "k" exc, "k" exc
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, k,
                         scan_endpoint::EXCLUSIVE, tup_lis, &nv));
    // "k" exc, "k" inc
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, k,
                         scan_endpoint::INCLUSIVE, tup_lis, &nv));
    // "k" exc, "k" inf
    ASSERT_EQ(status::ERR_BAD_USAGE,
              scan<char>(test_storage_name, k, scan_endpoint::EXCLUSIVE, k,
                         scan_endpoint::INF, tup_lis, &nv));
    ASSERT_EQ(leave(token), status::OK);
}

TEST_F(st, scan_multiple_same_null_char_key_1) { // NOLINT
    /**
   * scan against multiple put same null char key whose length is different each other
   * against single border node.
   */
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    constexpr std::size_t ary_size = 8;
    std::array<std::string, ary_size> k; // NOLINT
    std::array<std::string, ary_size> v; // NOLINT
    for (std::size_t i = 0; i < ary_size; ++i) {
        k.at(i).assign(i, '\0');
        v.at(i) = std::to_string(i);
        ASSERT_EQ(status::OK,
                  put(token, test_storage_name, std::string_view(k.at(i)),
                      v.at(i).data(), v.at(i).size()));
    }

    std::vector<std::tuple<std::string, char*, std::size_t>>
            tuple_list{}; // NOLINT
    constexpr std::size_t v_index = 1;
    for (std::size_t i = 0; i < ary_size; ++i) {
        scan<char>(test_storage_name, "", scan_endpoint::INF,
                   std::string_view(k.at(i)), scan_endpoint::INCLUSIVE,
                   tuple_list);
        for (std::size_t j = 0; j < i + 1; ++j) {
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j)),
                             v.at(j).data(), v.at(j).size()),
                      0);
        }
    }

    for (std::size_t i = ary_size - 1; i > 1; --i) {
        std::vector<std::pair<node_version64_body, node_version64*>> nv;
        scan<char>(test_storage_name, std::string_view(k.at(i)),
                   scan_endpoint::INCLUSIVE, "", scan_endpoint::INF, tuple_list,
                   &nv);
        ASSERT_EQ(tuple_list.size(), ary_size - i);
        ASSERT_EQ(tuple_list.size(), nv.size());
        for (std::size_t j = i; j < ary_size; ++j) {
            ASSERT_EQ(memcmp(std::get<v_index>(tuple_list.at(j - i)),
                             v.at(j).data(), v.at(j).size()),
                      0);
        }
    }
    ASSERT_EQ(leave(token), status::OK);
}

} // namespace yakushima::testing
