# How to use yakushima

This is a header only library by C++. So you can use very easily.

* Please link thread / tbb libraries.
* Please set include path to yakushima/include.
* Please include [yakushima/include/kvs.h](./../include/kvs.h) only!

Please refer to [yakushima/bench/CMakeLists.txt](./../bench/CMakeLists.txt).

## Example

### Sample program

```cpp
// You should include this only. This file is also interface.
#include <yakushima/include/kvs.h>

using namespace yakushima;

int main() {
    // Initialize of yakushima
    init();
    
    std::string table_name{"warehouse"};
    // You can build a separate tree.
    // create_storage, delete_storage, find_storage are not exclusively controlled, so call these from a single thread. Alternatively, do exclusive the call with user-defined exclusive control.
    create_storage(table_name);

    // Start using
    // You should declare token and use api with the token. This token means your session that protects your read data and save temporary data.
    Token token{};

    enter(token);
    std::string k("a");
    std::string v("b");
    // You can put some key-value.
    put(table_name, k, v.data(), v.size());
    // You can get some key-value.
    std::pair<char*, std::size_t> tuple = get<char>(table_name, k);
    // You can scan some key-values.
    std::vector<std::tuple<std::string, char*, std::size_t>> tuple_vec{};
    std::vector<std::pair<node_version64_body, node_version64*>> nv;
    scan<char>(table_name, "", scan_endpoint::INCLUSIVE, "", scan_endpoint::INCLUSIVE, tuple_vec, &nv);
    // You can remove some key-value.
    remove(token, table_name, k);

    // If you don't use it for a while, please leave
    leave(token);

    // Shutdown of yakushima
    fin();
    return 0;
}
```

### Compile sample program

```sh
g++ [sample_program_name] -I [path/to/yakushima directory] -lpthread -ltbb -std=c++1z
```

- -I [path/to/yakushima directory]
Take the appropriate include path.
Mutual inclusion of yakushima files uses relative paths, so you don't have to think about it.
Please pass the path so that the include directive you wrote is valid.

- lpthread -ltbb
yakushima assumes the use of thread library and tbb library.

- std=c++1z
yakushima assumes the use of c ++ 17.

![Overview](./pic/Overview.pdf)

For other usage, refer to the test file in the [test directory](./../test).

TODO : Add sentences as needed due to the current task priority.
