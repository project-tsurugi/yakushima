# Test

## unit-test
- kvs_test.cpp
  1. initialization.
- put_get_test.cpp
  1. single put/get to one border.
  2. multiple put/get same null char key whose length is different each other against single border node.
  3. multiple put/get same null char key whose length is different each other against multiple border, 
  which is across some layer.
  4. put repeatedly until first split of border node.
  5. put repeatedly until first split of interior node.
- delete_test.cpp
  1. delete against single put to one border.
  2. delete repeatedly until vanishing only one border node.
  3. delete repeatedly until vanishing two border nodes which is across layer.
  4. delete repeatedly until vanishing the structure which has one interior node having two border nodes.
  5. delete repeatedly until vanishing the structure which has one interior node having two interior nodes.
- scan_test.cpp
  1. scan against single put to one border.
  2. scan against multiple put same null char key whose length is different each other agaisnt single border node.
  3. scan against multiple put same null char key whose length is different each other agaisnt multiple border node, 
  which is across some layer.
  4. scan against the structure which it has interior node as root, and the interior has some border nodes as children.
  5. scan against the structure which it has interior node as root, root has two interior nodes as children, 
  and each of children has some border nodes as children.
- multi_thread_put_test.cpp<br>
put by multi threads and verify final state by scan.
  1. multiple put same null char key whose length is different each other against single border node.
  2. multiple put same null char key whose length is different each other against multiple border, which is across some layer.
  3. multiple put repeatedly until first split of border node.
  4. multiple put repeatedly until first split of interior node.
  
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
