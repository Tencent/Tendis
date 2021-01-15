add_library(commands STATIC command.cpp kv.cpp auth.cpp repl.cpp cluster.cpp debug.cpp hash.cpp list.cpp expire.cpp del.cpp set.cpp zset.cpp scan.cpp pf.cpp dump.cpp sort.cpp release.cpp script.cpp)
target_link_libraries(commands status skiplist network utils_common lock utils_common)

add_executable(command_test command_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	# command uses global variables to self-regist, linking flag whole-archive is required.
	target_link_libraries(command_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(command_test status server session network server_params stdc++fs utils_common gtest_main test_util ${SYS_LIBS})
else()
	target_link_libraries(command_test commands status server session network server_params utils_common gtest_main test_util ${SYS_LIBS})
	set_target_properties(command_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()
