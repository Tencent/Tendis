add_library(status STATIC status.cpp)

add_library(redis_port STATIC redis_port.cpp)

add_executable(status_test status_test.cpp)
target_include_directories(status_test PUBLIC "${PROJECT_SOURCE_DIR}/src/thirdparty/googletest-release-1.8.0/googletest/include")
target_link_libraries(status_test status gtest_main)
