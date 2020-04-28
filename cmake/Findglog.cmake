# Copyright 2018-2019 shark's fin project.
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

if(TARGET glog::glog)
    return()
endif()

find_library(glog_LIBRARY_FILE NAMES glog)
find_path(glog_INCLUDE_DIR NAMES glog/logging.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(glog DEFAULT_MSG
    glog_LIBRARY_FILE
    glog_INCLUDE_DIR)

if(glog_LIBRARY_FILE AND glog_INCLUDE_DIR)
    set(glog_FOUND ON)
    add_library(glog::glog SHARED IMPORTED)
    set_target_properties(glog::glog PROPERTIES
        IMPORTED_LOCATION "${glog_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${glog_INCLUDE_DIR}")
else()
    set(glog_FOUND OFF)
endif()

unset(glog_LIBRARY_FILE CACHE)
unset(glog_INCLUDE_DIR CACHE)
