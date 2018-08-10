add_library(status STATIC status.cpp)

add_executable(status_test status_test.cpp)
target_link_libraries(status_test status gtest_main)
add_test(NAME run_status_test COMMAND status_test)
