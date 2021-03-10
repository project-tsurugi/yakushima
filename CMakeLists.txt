
cmake_minimum_required(VERSION 3.10)

project(yakushima
        VERSION 0.0.1
        DESCRIPTION "yakushima"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build test programs" ON)
option(BUILD_DOCUMENTS "Build documents" ON)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" ON)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(ENABLE_JEMALLOC "use jemalloc for bench/malloc.cc as memory allocator." OFF)
option(PERFORMANCE_TOOLS "Enable tooling to measure performance" OFF)

find_package(Doxygen REQUIRED)
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
if(PERFORMANCE_TOOLS)
    find_package(performance-tools REQUIRED)
endif()
# for logging
find_package(Boost COMPONENTS filesystem)

include(FormatFilesWithClangFormat)
include(CompileOptions)
include(Tests)

# for ctest
if (BUILD_TESTS)
  enable_testing()
endif ()

add_subdirectory(include)
add_subdirectory(bench)
add_subdirectory(third_party)

if (BUILD_TESTS)
  add_subdirectory(test)
endif ()

if (BUILD_DOCUMENTS)
  add_subdirectory(doxygen)
endif ()
