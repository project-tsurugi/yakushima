/**
 * @file permutation_test.cpp
 */

#include <future>
#include <thread>

#include "gtest/gtest.h"

#include "permutation.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class permutation_test : public ::testing::Test {
protected:
  permutation_test() = default;

  ~permutation_test() = default;
};

TEST_F(permutation_test, basic_permutation_test) {
// basic member test.
  permutation per{};
  ASSERT_EQ(true, true); // test ending of function which returns void.
  ASSERT_EQ(per.get_cnk(), 0);
  ASSERT_EQ(per.set_cnk(1), status::OK);
  ASSERT_EQ(per.get_cnk(), 1);
  ASSERT_EQ(per.set_cnk(16), status::WARN_BAD_USAGE);
  ASSERT_EQ(per.get_cnk(), 1); // check invariant.
}

TEST_F(permutation_test, display) {
  permutation per{};
  ASSERT_EQ(true, true);
  //per.display();
  per.inc_key_num();
  ASSERT_EQ(true, true);
  //per.display();
}
}  // namespace yakushima::testing
