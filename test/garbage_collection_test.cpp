/**
 * @file garbage_collection.cpp
 */

#include <array>
#include <thread>
#include <tuple>

#include <glog/logging.h>

#include "gtest/gtest.h"

#include "epoch.h"
#include "kvs.h"
#include "log.h"

using namespace yakushima;

namespace yakushima::testing {
std::string test_storage_name{"1"}; // NOLINT

class garbage_collection : public ::testing::Test {
protected:
    void SetUp() override {
        init();
        create_storage(test_storage_name);
    }

    void TearDown() override { fin(); }
};

TEST_F(garbage_collection, gc) { // NOLINT
    Token token{};
    std::string k{"k"};
    std::string v{"k"};
    ASSERT_EQ(status::OK, enter(token));
    char** created_ptr{};
    ASSERT_EQ(status::OK, put(token, test_storage_name, k, v.data(), v.size(),
                              created_ptr, std::align_val_t(8)));
    ASSERT_EQ(status::OK, remove(token, test_storage_name, k));
    Epoch epo{epoch_management::get_epoch()};
    LOG(INFO) << log_location_prefix << "epoch is " << epo;
    ASSERT_EQ(status::OK, leave(token));
    while (epoch_management::get_epoch() - epo < 10) {
        /**
     * Put enough sleep and let the background thread gc.
     */
        _mm_pause();
    }
    LOG(INFO) << "final epoch is " << epoch_management::get_epoch();
}

} // namespace yakushima::testing
