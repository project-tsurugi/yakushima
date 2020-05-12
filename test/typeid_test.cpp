/**
 * @file typeid_test.cpp
 */

#include <memory>

#include "gtest/gtest.h"

#include "base_node.h"
#include "border_node.h"
#include "interior_node.h"

using namespace yakushima;
using std::cout;
using std::endl;

namespace yakushima::testing {

class typeid_test : public ::testing::Test {
protected:
  typeid_test() = default;

  ~typeid_test() = default;
};

TEST_F(typeid_test, typeid_check_super_derived) {
  std::unique_ptr<base_node> base_uptr(new base_node()); // u ... unique
  std::unique_ptr<border_node> border_uptr(new border_node());
  std::unique_ptr<interior_node> interior_uptr(new interior_node());

  ASSERT_EQ(typeid(base_uptr.get()), typeid(base_node*));
  ASSERT_EQ(typeid(border_uptr.get()), typeid(border_node*));
  ASSERT_EQ(typeid(interior_uptr.get()), typeid(interior_node*));

  base_node* base_nptr; // n ... normal
  base_nptr = base_uptr.get();
  ASSERT_EQ(typeid(*base_nptr), typeid(base_node));
  base_nptr = reinterpret_cast<base_node*>(border_uptr.get());
  ASSERT_EQ(typeid(*base_nptr), typeid(border_node));
  base_nptr = reinterpret_cast<base_node*>(interior_uptr.get());
  ASSERT_EQ(typeid(*base_nptr), typeid(interior_node));

  // test real object
  border_node border;
  interior_node interior;
  base_nptr = reinterpret_cast<base_node*>(&border);
  ASSERT_EQ(typeid(*base_nptr), typeid(border_node));
  base_nptr = reinterpret_cast<base_node*>(&interior);
  ASSERT_EQ(typeid(*base_nptr), typeid(interior_node));
}

}  // namespace yakushima::testing
