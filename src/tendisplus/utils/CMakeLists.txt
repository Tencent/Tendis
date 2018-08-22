add_library(status STATIC status.cpp)

add_library(redis_port STATIC redis_port.cpp)

add_executable(status_test status_test.cpp)
target_link_libraries(status_test status gtest_main)

add_library(time_util STATIC time.cpp)

add_executable(atomic_utility_test atomic_utility_test.cpp)
target_link_libraries(atomic_utility_test status gtest_main)

add_library(sync_point STATIC sync_point.cpp)
target_compile_definitions(sync_point PRIVATE WITH_SYNC_POINT)
