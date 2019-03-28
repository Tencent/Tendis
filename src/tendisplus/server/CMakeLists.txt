add_library(session session.cpp)
target_link_libraries(session status glog)

add_library(server server_entry.cpp)
target_link_libraries(server status network nwp time_util rocks_kvstore segment_mgr catalog repl_manager index_mgr pessimistic)

add_library(server_params server_params.cpp)
target_link_libraries(server_params status glog gtest_main)

add_library(segment_mgr segment_manager.cpp)
target_link_libraries(segment_mgr status session lock)

add_library(index_mgr index_manager.cpp)
target_link_libraries(index_mgr status session lock glog ${SYS_LIBS})

add_executable(index_mgr_test index_manager_test.cpp)
target_link_libraries(index_mgr_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
target_link_libraries(index_mgr_test index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})

add_executable(server_params_test server_params_test.cpp)
target_link_libraries(server_params_test server_params status glog ${SYS_LIBS})

add_executable(tendisplus main.cpp)
target_link_libraries(tendisplus -Wl,--whole-archive commands -Wl,--no-whole-archive)
target_link_libraries(tendisplus server_params server ${SYS_LIBS})
