#add_executable(network_test network_test.cpp)
#target_link_libraries(network_test network gtest_main)

add_library(server server_entry.cpp)
target_link_libraries(server status network nwp)
