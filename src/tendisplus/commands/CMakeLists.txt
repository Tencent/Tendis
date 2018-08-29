add_library(commands STATIC command.cpp get.cpp set.cpp auth.cpp repl.cpp)
target_link_libraries(commands status network utils_common)

add_library(commands_debug STATIC command.cpp get.cpp set.cpp auth.cpp repl.cpp)
target_compile_definitions(commands_debug PRIVATE WITH_SYNC_POINT)
target_link_libraries(commands_debug status network utils_common sync_point)

add_executable(commmand_test command_test.cpp)
target_compile_definitions(commmand_test PRIVATE WITH_SYNC_POINT)
# command uses global variables to self-regist, linking flag whole-archive is required.
target_link_libraries(commmand_test -Wl,--whole-archive commands_debug -Wl,--no-whole-archive)
target_link_libraries(commmand_test status network server_params stdc++fs utils_common sync_point gtest_main)
