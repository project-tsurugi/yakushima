# yakushima - Data structure (Concurrent Tree).

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* build related libararies - on Ubuntu, you can install with following command:

```
sudo apt update -y && sudo apt install -y $(cat build_tools/ubuntu.deps)
```

```sh
# retrieve third party modules
git submodule update --init --recursive
```

## Docerfile
```dockerfile
FROM ubuntu:18.04
RUN apt update -y && apt install -y $(cat build_tools/ubuntu.deps)
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` : never build test programs<br>
default : `ON`
* `-DBUILD_DOCUMENTS=OFF` : never build documents by doxygen<br>
default : `ON`
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON` : use formatting for source files<br>
default : `OFF`
* for debugging only<br>
  * `-DENABLE_SANITIZER=OFF` : disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)<br>
  default : `ON`
  * `-DENABLE_UB_SANITIZER=OFF` : enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)<br>
  default : `ON`
  * `-DENABLE_COVERAGE=ON` : enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)<br>
  default : `OFF`
* for bench
  * `-DENABLE_JEMALLOC=ON` : enable including jemalloc header for using as memory allocator via ld_preload.<br>
  default : `OFF`
  * `-DPERFORMANCE_TOOLS=ON` : enable tooling to measure benchmark performance.<br>
  default : `OFF`

You can use one sanitizer from 
 * address for AddressSanitizer
 * leak for LeakSanitizer
 * thread for ThreadSanitizer
 * undefined for UndefinedBehaviorSanitizer
 * memory for MemorySanitizer.

### run tests

```sh
ctest -V
```

### How to use
See [how_to_use.md](./docs/how_to_use.md) for details on how to use.

### generate documents

```sh
ninja doxygen
```

### code coverage

Run cmake with `-DENABLE_COVERAGE=ON` and run tests.
Dump the coverage information into html files with the following steps:
```
cd build
mkdir gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party/ -e ../test/'
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/yakushima-gcovr.html
```
Open gcovr-html/yakushima-gcovr.html to see the coverage report.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
