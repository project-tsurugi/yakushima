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

if(TARGET gflags::gflags)
    return()
endif()

find_library(gflags_LIBRARY_FILE NAMES gflags)
find_path(gflags_INCLUDE_DIR NAMES gflags/gflags.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gflags DEFAULT_MSG
    gflags_LIBRARY_FILE
    gflags_INCLUDE_DIR)

if(gflags_LIBRARY_FILE AND gflags_INCLUDE_DIR)
    set(gflags_FOUND ON)
    add_library(gflags::gflags SHARED IMPORTED ../include/scan_helper.h ../include/common_helper.h)
    set_target_properties(gflags::gflags PROPERTIES
        IMPORTED_LOCATION "${gflags_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${gflags_INCLUDE_DIR}")
else()
    set(gflags_FOUND OFF)
endif()

unset(gflags_LIBRARY_FILE CACHE)
unset(gflags_INCLUDE_DIR CACHE)
