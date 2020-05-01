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

TEST_F(mt_version_test, version_bitwise_operation) {
#if 0
  // about static function
  std::uint64_t num(0);
  ASSERT_EQ(node_version64::check_lock_bit(num), false);
  node_version64::set_lock_bit(num);
  ASSERT_EQ(true, true);
  ASSERT_EQ(node_version64::check_lock_bit(num), true);

  // about member function
  node_version64 ver;
  ASSERT_EQ(ver.get_version(), 0);
  ASSERT_EQ(ver.check_lock_bit(), false);
  ver.lock();
  ASSERT_EQ(true, true);
  ASSERT_EQ(ver.check_lock_bit(), true);
#endif
}

}  // namespace yakushima::testing
