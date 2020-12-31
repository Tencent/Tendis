add_library(network network.cpp blocking_tcp_client.cpp)
target_link_libraries(network session glog redis_port status server commands session_ctx)

add_library(nwp worker_pool.cpp)
target_link_libraries(nwp glog redis_port status server)

add_executable(network_test network_test.cpp)
target_link_libraries(network_test server network session gtest_main test_util ${SYS_LIBS})

add_library(session_ctx session_ctx.cpp)
target_link_libraries(session_ctx glog)

add_executable(worker_pool_test worker_pool_test.cpp)
target_link_libraries(worker_pool_test  gtest_main nwp test_util)
