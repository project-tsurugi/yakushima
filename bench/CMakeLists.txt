# Copyright 2019-2020 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

file(GLOB YAKUSHIMA_SOURCES
        "yakushima.cpp"
        )

add_executable(yakushima_bench
        ${YAKUSHIMA_SOURCES}
        )

target_link_libraries(yakushima_bench
        PRIVATE glog::glog
        PRIVATE Threads::Threads
        PRIVATE gflags::gflags
        PRIVATE ${tbb_prefix}tbb
        PRIVATE ${tbb_prefix}tbbmalloc
        PRIVATE ${tbb_prefix}tbbmalloc_proxy
        )

if (PERFORMANCE_TOOLS)
    target_link_libraries(yakushima_bench
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(yakushima_bench PUBLIC PERFORMANCE_TOOLS)
endif ()

target_include_directories(yakushima_bench
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

file(GLOB MALLOC_SOURCES
        "malloc.cpp"
        )

add_executable(malloc_bench
        ${MALLOC_SOURCES}
        )

target_link_libraries(malloc_bench
        PRIVATE glog::glog
        PRIVATE Threads::Threads
        PRIVATE gflags::gflags
        )

target_include_directories(malloc_bench
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/third_party/jemalloc/include
        PRIVATE ${gflags_INCLUDE_DIR}
        )

