# Test that put / delete / scan operations work in parallel

* multi_thread_put_delete_scan_one_border_test.cpp
  * Test the operations on one border node.
* multi_thread_put_delete_scan_two_border_test.cpp
  * Perform put / delete / scan operations in parallel. The state of the tree can range from nothing to two border

      nodes.

* multi_thread_put_delete_scan_one_interior_two_border_test.cpp
  * Perform put / delete / scan operations in parallel. The state of the tree can range from nothing to one interior

      node and two border nodes.

* multi_thread_put_delete_scan_one_interior_test.cpp
  * Perform put / delete / scan operations in parallel. The state of the tree can range from nothing to one interior node and many border nodes.
* multi_thread_put_delete_scan_many_interior_test.cpp
  * Perform put / delete / scan operations in parallel. The state of the tree can range from nothing to many interior nodes.
* multi_thread_put_delete_scan_test.cpp
  * Others.

## Restriction

Prefix the test file with multi_thread_put_delete_scan_ to avoid duplicate executable names.

## Todo

Separate files some file consumes a lot of time. Add kindly documents.
