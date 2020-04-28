
option(FORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD "If the command clang-format is available, format source files before each build. Turn this off if the build time is too slow." OFF)
find_program(CLANG_FORMAT_EXE clang-format)

function(clang_format target)
  if (CLANG_FORMAT_EXE)
    message(STATUS "Enable Clang-Format ${target}")
    get_target_property(MY_SOURCES ${target} SOURCES)
    message(STATUS "${target} : source files : ${MY_SOURCES}")
    add_custom_target(
      "${target}_format-with-clang-format"
      COMMAND "${CLANG_FORMAT_EXE}" -i -style=file ${MY_SOURCES}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      )
    if (FORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD)
      add_dependencies(${target} "${target}_format-with-clang-format")
    endif()
  endif()
endfunction()

