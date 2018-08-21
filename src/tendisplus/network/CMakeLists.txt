add_library(network network.cpp)
target_compile_options(network PUBLIC -DASIO_STANDALONE)
target_link_libraries(network pthread glog redis_port status server session_ctx)

add_library(nwp worker_pool.cpp)
target_compile_options(nwp PUBLIC -DASIO_STANDALONE)
target_link_libraries(nwp pthread glog redis_port status server)

add_executable(network_test network_test.cpp)
target_link_libraries(network_test network gtest_main)

add_library(session_ctx session_ctx.cpp)
