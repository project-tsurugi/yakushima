/**
 * @file link_or_value_test.cpp
 */

#include <future>
#include <thread>

#include "gtest/gtest.h"

#include "link_or_value.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class link_or_value_test : public ::testing::Test {
protected:
  link_or_value_test() = default;
  ~link_or_value_test() = default;
};

TEST_F(link_or_value_test, link_or_value_test) {
  link_or_value lv;
}

}  // namespace yakushima::testing
