/**
 * @file storage_test.cpp
 */

#include "gtest/gtest.h"

#include "kvs.h"
#include "storage.h"

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

TEST_F(st, simple_create_storage) {// NOLINT
    std::string st1{"st1"};
    ASSERT_EQ(status::OK, create_storage(st1));
    std::vector<tree_instance*> tuple;
    ASSERT_EQ(status::OK, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 1);
    tree_instance* ret_ti;
    ASSERT_EQ(find_storage(st1, &ret_ti), status::OK);
}

TEST_F(st, simple_delete_storage) {// NOLINT
    std::string st1{"st1"};
    ASSERT_EQ(status::OK, create_storage(st1));
    ASSERT_EQ(delete_storage(st1), status::OK);
    std::vector<tree_instance*> tuple;
    ASSERT_EQ(status::WARN_NOT_EXIST, list_storages(tuple));
    ASSERT_EQ(tuple.size(), 0);
    tree_instance* ret_ti;
    ASSERT_EQ(find_storage(st1, &ret_ti), status::WARN_NOT_EXIST);
}

TEST_F(st, simple_find_storage) {// NOLINT
    std::string st1{"st1"};
    std::string st2{"st2"};
    ASSERT_EQ(status::OK, create_storage(st1));
    tree_instance* ret_ti;
    ASSERT_EQ(find_storage(st1, &ret_ti), status::OK);
    ASSERT_EQ(find_storage(st2, &ret_ti), status::WARN_NOT_EXIST);
}

TEST_F(st, simple_list_storage) {// NOLINT
    std::string st1{"st1"};
    std::string st2{"st2"};
    std::vector<tree_instance*> tuple;
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

}// namespace yakushima::testing
