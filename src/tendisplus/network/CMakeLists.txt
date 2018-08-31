add_library(network network.cpp blocking_tcp_client.cpp)
target_link_libraries(network pthread glog redis_port status server commands session_ctx)

add_library(nwp worker_pool.cpp)
target_link_libraries(nwp pthread glog redis_port status server)

add_executable(network_test network_test.cpp)
target_link_libraries(network_test network gtest_main)

add_library(session_ctx session_ctx.cpp)
