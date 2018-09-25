add_library(server server_entry.cpp)
target_link_libraries(server status network nwp time_util rocks_kvstore segment_mgr catalog repl_manager pessimistic)

add_library(server_params server_params.cpp)
target_link_libraries(server_params status glog gtest_main)

add_library(segment_mgr segment_manager.cpp)

add_executable(server_params_test server_params_test.cpp)
target_link_libraries(server_params_test server_params status glog)

add_executable(tendisplus main.cpp)
target_link_libraries(tendisplus -Wl,--whole-archive commands -Wl,--no-whole-archive)
target_link_libraries(tendisplus server_params server)
