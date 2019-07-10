add_library(commands STATIC command.cpp kv.cpp auth.cpp repl.cpp debug.cpp hash.cpp list.cpp expire.cpp del.cpp set.cpp zset.cpp scan.cpp pf.cpp dump.cpp sort.cpp)
target_link_libraries(commands status skiplist network utils_common lock base64)

add_executable(commmand_test command_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	# command uses global variables to self-regist, linking flag whole-archive is required.
	target_link_libraries(commmand_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(commmand_test status server session network server_params stdc++fs utils_common gtest_main test_util ${SYS_LIBS})
else()
	target_link_libraries(commmand_test commands status server session network server_params utils_common gtest_main test_util ${SYS_LIBS})
	set_target_properties(commmand_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()
