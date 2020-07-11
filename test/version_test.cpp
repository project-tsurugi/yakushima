/**
 * @file version_test.cpp
 */

#include <future>

#include "gtest/gtest.h"

#include "version.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class version_test : public ::testing::Test {
protected:
  version_test() = default;

  ~version_test() = default;
};

TEST_F(version_test, operator_node_version64_body_test) {
  node_version64_body b1{};
  node_version64_body b2{};
  b1.init();
  b2.init();
  ASSERT_EQ(b1, b2);
  b1.set_locked(!b1.get_locked());
  ASSERT_NE(b1, b2);
}

TEST_F(version_test, basic_node_version_test) {
  // single update test.
  {
    node_version64 ver;
    auto vinsert_inc_100 = [&ver]() {
      for (auto i = 0; i < 100; ++i) {
        ver.atomic_inc_vinsert();
      }
    };
    vinsert_inc_100();
    ASSERT_EQ(ver.get_body().get_vinsert(), 100);
  }

  // concurrent update test.
  {
    node_version64 ver;
    auto vinsert_inc_100 = [&ver]() {
      for (auto i = 0; i < 100; ++i) {
        ver.atomic_inc_vinsert();
      }
    };
    std::future<void> f = std::async(std::launch::async, vinsert_inc_100);
    vinsert_inc_100();
    f.wait();
    ASSERT_EQ(ver.get_body().get_vinsert(), 200);
  }

}

TEST_F(version_test, display) {
  node_version64_body vb{};
  vb.init();
  ASSERT_EQ(true, true);
  //vb.display();

  node_version64 v{};
  v.init();
  ASSERT_EQ(true, true);
  //v.display();
}

}  // namespace yakushima::testing
