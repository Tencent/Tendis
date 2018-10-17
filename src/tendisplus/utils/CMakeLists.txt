add_library(status STATIC status.cpp)

add_library(redis_port STATIC redis_port.cpp)
target_link_libraries(redis_port glog)

add_executable(status_test status_test.cpp)
target_link_libraries(status_test status gtest_main ${SYS_LIBS})

add_library(time_util STATIC time.cpp)

add_library(string_util STATIC string.cpp)

add_executable(atomic_utility_test atomic_utility_test.cpp)
target_link_libraries(atomic_utility_test status gtest_main ${SYS_LIBS})

add_library(sync_point STATIC sync_point.cpp)
target_compile_definitions(sync_point PRIVATE WITH_SYNC_POINT)

add_library(utils_common STATIC status.cpp redis_port.cpp time.cpp string.cpp)
