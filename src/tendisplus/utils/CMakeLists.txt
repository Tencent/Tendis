add_library(status STATIC status.cpp)
target_link_libraries(status glog)

add_library(redis_port STATIC lzf_d.cpp redis_port.cpp hyperloglog.cpp)
target_link_libraries(redis_port glog)

add_executable(status_test status_test.cpp)
target_link_libraries(status_test status gtest_main ${SYS_LIBS})

add_library(time_util STATIC time.cpp)

add_executable(atomic_utility_test atomic_utility_test.cpp)
target_link_libraries(atomic_utility_test status gtest_main ${SYS_LIBS})

add_library(sync_point STATIC sync_point.cpp)

if(CMAKE_COMPILER_IS_GNUCC)
	set(STD "")
else()
	set(STD unistd.cpp)
	# fake rt(a dummy lib)
	add_library(rt STATIC dummy.cpp)
endif()

add_library(utils_common STATIC status.cpp lzf_d.cpp redis_port.cpp hyperloglog.cpp time.cpp string.cpp base64.cpp param_manager.cpp cursor_map.cpp ${STD})
target_link_libraries(utils_common glog varint)

add_library(test_util STATIC test_util.cpp)
target_link_libraries(test_util utils_common server_params)

add_executable(utils_common_test utils_common_test.cpp)
target_link_libraries(utils_common_test utils_common gtest_main ${SYS_LIBS})

