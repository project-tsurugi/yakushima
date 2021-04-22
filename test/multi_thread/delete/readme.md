# Test that delete operations work in parallel

* multi_thread_delete_200_key_test.cpp
  * Perform delete operations on 200 keys in parallel.
* multi_thread_delete_test.cpp
  * Perform delete operations in parallel.

## Restriction

Prefix the test file with multi_thread_delete_ to avoid duplicate executable names.

## Todo

Separate files some file consumes a lot of time. Add kindly documents.
