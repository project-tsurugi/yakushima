# Test

## unit-test
- kvs_test.cpp
  1. initialization.
- put_get_test.cpp
  1. single put/get to one border.
  2. multiple put/get same null char key whose length is different each other 
  against single border node.
  3. test of 2 which is using shuffled data.
  4. multiple put/get same null char key whose length is different each other 
  against multiple border, which is across some layer.
  5. test of 4 which is using shuffled data.
  6. put repeatedly until first split of border node.
  7. test of 6 which is using shuffled data.
  8. put repeatedly until first split of interior node.
  9. test of 8 which is using shuffled data.
- delete_test.cpp
  1. delete against single put to one border.
  2. delete repeatedly until vanishing only one border node.
  3. test of 2 which is using shuffled data.
  4. delete repeatedly until vanishing two border nodes which is across layer.
  5. test of 4 which is using shuffled data.
  6. delete repeatedly until vanishing the structure which has one interior node having two border nodes.
  7. test of 6 which is using shuffled data.
  8. delete repeatedly until vanishing the structure which has one interior node having two interior nodes.
  9. test of 8 which is using shuffled data.
- scan_test.cpp
  - `basic_usage`
  - `scan_against_single_put_null_key_to_one_border` 
  - `scan_against_single_put_non_null_key_to_one_border`  
  - `scan_multiple_same_null_char_key_1`<br>
  scan against multiple put same null char key whose length is different each other against single border node.
  - `scan_multiple_same_null_char_key_2`<br>
  scan against multiple put same null char key whose length is different each other against multiple border node, 
  which is across some layer.
  - `scan_against_1_interior_some_border`<br>
  scan against the structure which it has interior node as root, and the interior has some border nodes as children.
  - `scan_against_1_interior_2_interior_some_border`<br>
  scan against the structure which it has interior node as root, root has two interior nodes as children, 
  and each of children has some border nodes as children.
  - `Scan with prefix for 2 layers`<br>
- multi_thread_put_test.cpp<br>
put by multi threads and verify final state by scan.
  - `concurrent_put_against_single_border`<br>
  multiple put same null char key whose length is different each other against single border node.
  - `concurrent_put_against_single_border_with_shuffle`<br>
  test of above which is using shuffled data.
  - `concurrent_put_against_multiple_border_multiple_layer`
  multiple put same null char key whose length is different each other against multiple border, which is across some layer.
  - `concurrent_put_against_multiple_border_multiple_layer_with_shuffle`<br>
  test of above which is using shuffled data.
  - `concurrent_put_untill_first_split_border`<br>
  multiple put repeatedly until first split of border node.
  - `concurrent_put_untill_first_split_border_with_shuffle`<br>
  test of above which is using shuffled data.
  - `concurrent_put_many_split_border`<br>
  multiple put repeatedly between first split of border node and first split of interior node.
  - `concurrent_put_many_split_border_with_shuffle`<br>
  test of above which is using shuffled data.
  - `concurrent_put_untill_first_split_interior`<br>
  multiple put repeatedly until first split of interior node.
  - `concurrent_put_untill_first_split_interior_with_shuffle`<br>
  test of above which is using shuffled data.
- multi_thread_delete_test.cpp<br>
test about concurrent delete along with 
multi_thread_put_test.cpp.
- multi_thread_put_delete_test.cpp<br>
  - `concurrent_put_delete_against_single_border`<br>
  concurrent put/delete same null char key slices and different key length to single border by multi threads.
  - `concurrent_put_delete_against_single_border_with_shuffled_data`<br>
  concurrent_put_delete_against_single_border variant which is the test using shuffle order data.
  - `concurrent_put_delete_key_using_null_char_against_multiple_border`<br>
  multiple put same null char key whose length is different each other against multiple border, which is across some layer.
  - `concurrent_put_delete_key_using_null_char_against_multiple_border_with_shuffled_data`<br>
  concurrent_put_delete_key_using_null_char_against_multiple_border variant which is the test using shuffle order data.
  - `concurrent_put_delete_between_none_and_interior`<br>
  The number of puts that can be split border only once and the deletes are repeated in multiple threads.
  - `concurrent_put_delete_between_none_and_interior_in_second_layer`<br>
  The number of puts that can be split only once and the deletes are repeated in multiple threads. This situations in second layer.
  - `concurrent_put_delete_between_none_and_interior_in_first_layer_with_shuffle`<br>
  The number of puts that can be split only once and the deletes are repeated in multiple threads. Use shuffled data.
  - `concurrent_put_delete_between_none_and_interior_in_second_layer_with_shuffle`<br>
  The number of puts that can be split only once and the deletes are repeated in multiple threads. Use shuffled data.
  - `concurrent_put_delete_between_none_and_sprit_interior_in_first_layer_with_shuffle`<br>
  concurrent put/delete in the state between none to split of interior, which is using shuffled data.
  - `concurrent_put_delete_between_none_and_sprit_interior_in_second_layer_with_shuffle`<br>
  concurrent put/delete in the state between none to split of interior, which is using shuffled data.
  - `concurrent_put_delete_between_none_and_many_split_of_interior`<br>
  - `concurrent_put_delete_between_none_and_many_split_of_interior_with_shuffle`<br>
  - `concurrent_put_delete_between_none_and_many_split_of_interior_with_shuffle_in_multi_layer`<br>
- multi_thread_put_delete_get_test.cpp<br>
test about concurrent put/delete/get along with 
multi_thread_put_test.cpp.
- multi_thread_put_delete_scan_test.cpp<br>
test about concurrent put/delete/scan along with 
multi_thread_put_test.cpp.

## developer-test
- base_node_test.cpp
- border_node_test.cpp
- clock_test.cpp
- compare_test.cpp
- delete_test.cpp
- interior_node_test.cpp
- link_or_value_test.cpp
- permutation_test.cpp
- thread_info_test.cpp
- typeid_test.cpp
- version_test.cpp
