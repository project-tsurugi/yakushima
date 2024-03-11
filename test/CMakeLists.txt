# Copyright 2019-2019 tsurugi project.
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

if (BUILD_ONLY_WD_TEST)
  message("Build only working directory about tests.")
  file(GLOB TEST_SOURCES
          "wd/*.cpp"
          )
else ()
  file(GLOB TEST_SOURCES
          "*.cpp"
          "multi_thread/*.cpp"
          "multi_thread/delete/*.cpp"
          "multi_thread/put/*.cpp"
          "multi_thread/put_delete/*.cpp"
          "multi_thread/put_delete_scan/*.cpp"
          "multi_thread/put_delete_get/*.cpp"
          "put/*.cpp"
          "put_get/*.cpp"
          "get/*.cpp"
          "scan/*.cpp"
          "delete/*.cpp"
          "display/*.cpp"
          "delete_get_put_scan/*.cpp"
          "delete_put/*.cpp"
          "tsurugi_issue/*.cpp"
          )
endif ()

register_tests(
        TARGET yakushima_test
        DEPENDS
          PRIVATE glog::glog
          PRIVATE ${tbb_prefix}tbb
          PRIVATE ${tbb_prefix}tbbmalloc
          PRIVATE ${tbb_prefix}tbbmalloc_proxy
        SOURCES ${TEST_SOURCES}
)
