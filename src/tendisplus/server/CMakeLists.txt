add_library(session session.cpp)
target_link_libraries(session status glog)

add_library(server server_entry.cpp)
target_link_libraries(server status network nwp time_util rocks_kvstore segment_mgr catalog repl_manager migrate gc_mgr index_mgr cluster_mgr pessimistic server_params)

add_library(server_params server_params.cpp)
target_link_libraries(server_params status glog server gtest_main)

add_library(segment_mgr segment_manager.cpp)
target_link_libraries(segment_mgr status session lock)

add_library(index_mgr index_manager.cpp)
target_link_libraries(index_mgr status session lock glog ${SYS_LIBS})

add_executable(index_mgr_test index_manager_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(index_mgr_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(index_mgr_test index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
else()
	target_link_libraries(index_mgr_test commands index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
	set_target_properties(index_mgr_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()

add_executable(repl_test repl_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(repl_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(repl_test index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
else()
	target_link_libraries(repl_test commands index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
	set_target_properties(repl_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()

add_executable(restore_test restore_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(restore_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(restore_test index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
else()
	target_link_libraries(restore_test commands index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
	set_target_properties(restore_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()

add_executable(server_params_test server_params_test.cpp)
target_link_libraries(server_params_test server_params status glog utils_common ${SYS_LIBS})

add_executable(tendisplus main.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	# command uses global variables to self-regist, linking flag whole-archive is required.
	target_link_libraries(tendisplus -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(tendisplus server_params server ${SYS_LIBS})
	set_target_properties(tendisplus PROPERTIES LINK_FLAGS "-static-libgcc -static-libstdc++") # -static-libasan
else()
	target_link_libraries(tendisplus commands server_params server ${SYS_LIBS})
	set_target_properties(tendisplus PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()
