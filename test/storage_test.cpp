/**
 * @file storage_test.cpp
 */

#include <cstring>

#include "gtest/gtest.h"

#include "kvs.h"
#include "storage.h"

using namespace yakushima;

namespace yakushima::testing {

class st : public ::testing::Test {
    void SetUp() override { init(); }

    void TearDown() override { fin(); }
};

TEST_F(st, simple_create_storage) { // NOLINT
    std::string st1{"st1"};
    ASSERT_EQ(status::OK, create_storage(st1));
    std::vector<std::pair<std::string, tree_instance*>> tuple;
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 1);
    tree_instance* ret_ti{};
    ASSERT_EQ(find_storage(st1, &ret_ti), status::OK);
}

TEST_F(st, simple_delete_storage) { // NOLINT
    std::string st1{"st1"};
    ASSERT_EQ(status::OK, create_storage(st1));
    ASSERT_EQ(delete_storage(st1), status::OK);
    std::vector<std::pair<std::string, tree_instance*>> tuple;
    ASSERT_EQ(status::WARN_NOT_EXIST, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 0);
    tree_instance* ret_ti{};
    ASSERT_EQ(find_storage(st1, &ret_ti), status::WARN_NOT_EXIST);
}

TEST_F(st, simple_find_storage) { // NOLINT
    std::string st1{"st1"};
    std::string st2{"st2"};
    ASSERT_EQ(status::OK, create_storage(st1));
    tree_instance* ret_ti{};
    ASSERT_EQ(find_storage(st1, &ret_ti), status::OK);
    ASSERT_EQ(find_storage(st1), status::OK);
    ASSERT_EQ(find_storage(st2, &ret_ti), status::WARN_NOT_EXIST);
}

TEST_F(st, simple_list_storage) { // NOLINT
    std::string st1{"st1"};
    std::string st2{"st2"};
    std::vector<std::pair<std::string, tree_instance*>> tuple;
    ASSERT_EQ(status::WARN_NOT_EXIST, list_storages(tuple));
    ASSERT_EQ(status::OK, create_storage(st1));
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 1);
    ASSERT_EQ(create_storage(st2), status::OK);
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 2);
    ASSERT_EQ(delete_storage(st1), status::OK);
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 1);
    ASSERT_EQ(delete_storage(st2), status::OK);
    ASSERT_EQ(status::WARN_NOT_EXIST, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 0);
    ASSERT_EQ(status::OK, create_storage(st1));
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 1);
}

TEST_F(st, simple_operate_some_storage) { // NOLINT
    std::string st1{"st1"};
    std::string st2{"st2"};
    std::string k1{"k1"};
    std::string k2{"k2"};
    std::string k3{"k3"};
    std::string k4{"k4"};
    std::string k5{"k5"};
    std::string k6{"k6"};
    std::string v{"v"};
    ASSERT_EQ(status::OK, create_storage(st1));
    ASSERT_EQ(status::OK, create_storage(st2));
    Token token{};
    ASSERT_EQ(enter(token), status::OK);
    ASSERT_EQ(put(token, st1, k1, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(token, st1, k2, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(token, st1, k3, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(token, st2, k4, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(token, st2, k5, v.data(), v.size()), status::OK);
    ASSERT_EQ(put(token, st2, k6, v.data(), v.size()), status::OK);
    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_list;
    constexpr std::size_t key_index{0};
    ASSERT_EQ(status::OK, scan(st1, "", scan_endpoint::INF, "",
                               scan_endpoint::INF, tuple_list));
    ASSERT_EQ(tuple_list.size(), 3);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(0)).data(), k1.data(),
                     k1.size()),
              0);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(1)).data(), k2.data(),
                     k1.size()),
              0);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(2)).data(), k3.data(),
                     k1.size()),
              0);
    ASSERT_EQ(status::OK, scan(st2, "", scan_endpoint::INF, "",
                               scan_endpoint::INF, tuple_list));
    ASSERT_EQ(tuple_list.size(), 3);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(0)).data(), k4.data(),
                     k1.size()),
              0);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(1)).data(), k5.data(),
                     k1.size()),
              0);
    ASSERT_EQ(memcmp(std::get<key_index>(tuple_list.at(2)).data(), k6.data(),
                     k1.size()),
              0);
    ASSERT_EQ(status::OK, leave(token));
}

} // namespace yakushima::testing
