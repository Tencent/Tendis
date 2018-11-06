add_library(commands STATIC command.cpp kv.cpp auth.cpp repl.cpp debug.cpp hash.cpp list.cpp expire.cpp del.cpp set.cpp zset.cpp)
target_link_libraries(commands status skiplist network utils_common lock)

add_library(commands_debug STATIC command.cpp kv.cpp auth.cpp repl.cpp debug.cpp hash.cpp list.cpp expire.cpp del.cpp set.cpp zset.cpp)
target_compile_definitions(commands_debug PRIVATE WITH_SYNC_POINT)
target_link_libraries(commands_debug status skiplist network utils_common lock sync_point)

add_executable(commmand_test command_test.cpp)
target_compile_definitions(commmand_test PRIVATE WITH_SYNC_POINT)
# command uses global variables to self-regist, linking flag whole-archive is required.
target_link_libraries(commmand_test -Wl,--whole-archive commands_debug -Wl,--no-whole-archive)
target_link_libraries(commmand_test status server session network server_params stdc++fs utils_common sync_point gtest_main ${SYS_LIBS})
