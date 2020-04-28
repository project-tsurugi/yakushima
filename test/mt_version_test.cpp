#include "gtest/gtest.h"

#include "mt_version.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class mt_version_test : public ::testing::Test {
protected:
  mt_version_test() {}
  ~mt_version_test() {}
};

TEST_F(mt_version_test, constructor) {
  node_version64 ver;
}

}  // namespace yakushima::testing
