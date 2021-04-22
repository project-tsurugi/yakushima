# Test that put / delete / get operations work in parallel

* multi_thread_put_delete_get_one_border_test.cpp
  * Test the operations on one border node.
* multi_thread_put_delete_get_two_border_test.cpp
  * Perform put / delete / get operations in parallel. The state of the tree can range from nothing to two border nodes.
* multi_thread_put_delete_get_one_interior_two_border_test.cpp
  * Perform put / delete / get operations in parallel. The state of the tree can range from nothing to one interior node and two border nodes.
* multi_thread_put_delete_get_one_interior_many_border_test.cpp
  * Perform put / delete / get operations in parallel. The state of the tree can range from nothing to one interior node and many border nodes.
* multi_thread_put_delete_get_many_interior_test.cpp
  * Perform put / delete / get operations in parallel. The state of the tree can range from nothing to many interior nodes and many border nodes.
* multi_thread_put_delete_get_test.cpp
  * Others.

## Restriction

Prefix the test file with multi_thread_put_delete_get_ to avoid duplicate executable names.

## Todo

Separate files some file consumes a lot of time. Add kindly documents.
