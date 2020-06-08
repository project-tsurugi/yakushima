# Test

## unit-test
- kvs_test.cpp
  - test contents
  1. initialization.
  2. single put/get to one border.
  3. multiple put/get same null char key whose length is different each other against single border node.
  4. multiple put/get same null char key whose length is different each other against multiple border, which is across some layer.
  5. put repeatedly until first split of border node.
  6. put repeatedly until first split of interior node.
  7. delete until vanishing only one border node whose has 1 key.
  8. delete repeatedly until vanishing only one border node.
  9. delete repeatedly until vanishing two border nodes which is across layer.
  10. delete repeatedly until vanishing the structure which has one interior node having two border nodes.
  11. delete repeatedly until vanishing the structure which has one interior node having two interior nodes.

## developer-test
- base_node_test.cpp
- border_node_test.cpp
- clock_test.cpp
- interior_node_test.cpp
- link_or_value_test.cpp
- permutation_test.cpp
- thread_info_test.cpp
- typeid_test.cpp
- version_test.cpp
