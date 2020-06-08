# yakushima - Storage Engine.

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* build related libararies - on Ubuntu, you can install with following command:

```
apt update -y && apt install -y git build-essential cmake ninja-build doxygen libgflags-dev libgoogle-glog-dev gnuplot libboost-filesystem-dev clang-tidy-8 gcovr
```

```sh
# retrieve third party modules
git submodule update --init --recursive
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON` - use formatting for source files
* for debugging only<br>
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
* `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries
<br><br>

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
