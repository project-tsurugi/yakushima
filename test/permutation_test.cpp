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
  permutation_body perbody;
  perbody.init();
  ASSERT_EQ(true, true); // test ending of function which returns void.
  ASSERT_EQ(per.get_cnk(), 0);
  perbody.set_cnk(1);
  ASSERT_EQ(true, true); // test ending of function which returns void.
  per.set_body(perbody);
  ASSERT_EQ(true, true); // test ending of function which returns void.
  ASSERT_EQ(per.get_cnk(), 1);
}

}  // namespace yakushima::testing
