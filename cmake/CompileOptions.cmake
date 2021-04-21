set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

set(sanitizers "address")
if (ENABLE_UB_SANITIZER)
  set(sanitizers "${sanitizers},undefined")
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  #do nothing for gcc
elseif (CMAKE_CXX_COMPILER_ID_MATCHES "^(Clang|AppleClang)$")
  set(sanitizers "${sanitizers},nullability")
else ()
  message(FATAL_ERROR "unsupported compiler ${CMAKE_CXX_COMPILER_ID}")
endif ()

if (ENABLE_SANITIZER)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=${sanitizers}")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize=alignment")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize-recover=${sanitizers}")
endif ()
if (ENABLE_COVERAGE)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage")
endif ()

cmake_host_system_information(RESULT cores QUERY NUMBER_OF_LOGICAL_CORES)

add_definitions(-D YAKUSHIMA_EPOCH_TIME=40)

if (NOT DEFINED YAKUSHIMA_MAX_PARALLEL_SESSIONS)
  add_definitions(-D YAKUSHIMA_MAX_PARALLEL_SESSIONS=${cores})
  message("YAKUSHIMA_MAX_PARALLEL_SESSIONS is default (${cores})")
else ()
  add_definitions(-D YAKUSHIMA_MAX_PARALLEL_SESSIONS=${YAKUSHIMA_MAX_PARALLEL_SESSIONS})
  message("YAKUSHIMA_MAX_PARALLEL_SESSIONS is ${YAKUSHIMA_MAX_PARALLEL_SESSIONS}")
endif ()

add_definitions(-D YAKUSHIMA_LINUX)

if (ENABLE_JEMALLOC)
  add_definitions(-D ENABLE_JEMALLOC)
endif ()

function(set_compile_options target_name)
  target_compile_options(${target_name}
  PRIVATE -Wall -Wextra -Werror)
endfunction(set_compile_options)
