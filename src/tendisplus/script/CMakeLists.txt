add_library(script STATIC script_manager.cpp lua_state.cpp sha1.c rand.cpp)
target_link_libraries(script glog lua)

add_executable(script_test script_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(script_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(script_test script index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
else()
	target_link_libraries(script_test script commands index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
	set_target_properties(script_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()

