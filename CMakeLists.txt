
cmake_minimum_required(VERSION 3.10)

project(yakushima
        VERSION 1.0.0
        DESCRIPTION "yakushima"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_BENCHMARK "Build benchmark programs" ON)
option(BUILD_TESTS "Build test programs" ON)
option(BUILD_DOCUMENTS "Build documents" ON)
option(BUILD_ONLY_WD_TEST "Build only working directory about test for dev" OFF)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" ON)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(ENABLE_JEMALLOC "use jemalloc for bench/malloc.cc as memory allocator." OFF)
option(PERFORMANCE_TOOLS "Enable tooling to measure performance" OFF)

find_package(Doxygen REQUIRED)
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(TBB
  COMPONENTS tbb tbbmalloc tbbmalloc_proxy
  CONFIG QUIET
)
if (DEFINED TBB_CONFIG)
  set(tbb_prefix "TBB::")
else()
  find_package(TBB REQUIRED)
  set(tbb_prefix "")
endif()
if(PERFORMANCE_TOOLS)
    find_package(performance-tools REQUIRED)
endif()
# for logging
find_package(Boost COMPONENTS filesystem)

include(GNUInstallDirs)
include(CompileOptions)
include(Tests)
include(CMakePackageConfigHelpers)

# for ctest
if (BUILD_TESTS)
  enable_testing()
endif ()

set(export_name "yakushima")
set(package_name "yakushima")

add_library(yakushima INTERFACE)

target_include_directories(yakushima
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/${export_name}>
)

install(
    TARGETS
        yakushima
    EXPORT
        ${export_name}
)

install(
    DIRECTORY include/
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${export_name}/yakushima/include"
    COMPONENT Development
    FILES_MATCHING PATTERN "*.h"
)

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
    EXPORT ${package_name}
    FILE ${package_name}-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

add_subdirectory(third_party)

if (BUILD_BENCHMARK)
    add_subdirectory(bench)
endif ()

if (BUILD_TESTS)
  add_subdirectory(test)
endif ()

if (BUILD_DOCUMENTS)
  add_subdirectory(doxygen)
endif ()
