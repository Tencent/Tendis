add_library(commands STATIC command.cpp kv.cpp auth.cpp repl.cpp debug.cpp hash.cpp list.cpp expire.cpp del.cpp set.cpp zset.cpp scan.cpp)
target_link_libraries(commands status skiplist network utils_common lock)

add_executable(commmand_test command_test.cpp)
# command uses global variables to self-regist, linking flag whole-archive is required.
target_link_libraries(commmand_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
target_link_libraries(commmand_test status server session network server_params stdc++fs utils_common gtest_main ${SYS_LIBS})
