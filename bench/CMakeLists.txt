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
  "yakushima.cc"
)

add_executable(yakushima_bench
  ${YAKUSHIMA_SOURCES}
)

target_link_libraries(yakushima_bench
  PRIVATE Threads::Threads
  PRIVATE gflags::gflags
)

target_include_directories(yakushima_bench
  PRIVATE .
  PRIVATE ./include
  PRIVATE ./../src
  PRIVATE ${gflags_INCLUDE_DIR}
)