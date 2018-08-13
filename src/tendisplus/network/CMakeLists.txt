add_library(network network.cpp)
target_compile_options(network PUBLIC -DASIO_STANDALONE)
target_link_libraries(network pthread glog redis_port status)

add_executable(network_test network_test.cpp)
target_link_libraries(network_test network gtest_main)
