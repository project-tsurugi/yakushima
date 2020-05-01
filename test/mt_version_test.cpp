/**
 * @file mt_version_test.cpp
 */

#include <future>
#include <thread>

#include "gtest/gtest.h"

#include "mt_version.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class mt_version_test : public ::testing::Test {
protected:
  mt_version_test() = default;
  ~mt_version_test() = default;
};

TEST_F(mt_version_test, basic_node_version_test) {
// basic member test.
{
    node_version64 ver;
    node_version64_body verbody;
    ASSERT_EQ(ver.get_body().get_locked(), false);
    verbody = ver.get_body();
    verbody.set_locked(true);
    ver.set(verbody);
    ASSERT_EQ(ver.get_body().get_locked(), true);
}

  // single update test.
{
  node_version64 ver;
  auto vinsert_inc_100 = [&ver]() {
    for (auto i = 0; i < 100; ++i) {
      ver.increment_vinsert();
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
      ver.increment_vinsert();
    }
  };
  std::future<void> f = std::async(std::launch::async, vinsert_inc_100);
  vinsert_inc_100();
  f.wait();
  ASSERT_EQ(ver.get_body().get_vinsert(), 200);
}

}

}  // namespace yakushima::testing
