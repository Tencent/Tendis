add_library(server server_entry.cpp)
target_link_libraries(server status network nwp)

add_library(server_params server_params.cpp)
target_link_libraries(server_params status glog gtest_main)

add_executable(server_params_test server_params_test.cpp)
target_link_libraries(server_params_test server_params status glog)

add_executable(tendisplus main.cpp)
target_link_libraries(tendisplus server_params server)

