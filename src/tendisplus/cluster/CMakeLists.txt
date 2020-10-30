add_library(migrate STATIC migrate_manager.cpp migrate_sender.cpp migrate_receiver.cpp)
target_link_libraries(migrate status glog network catalog kvstore ${SYS_LIBS})


add_library(gc_mgr STATIC gc_manager.cpp)
target_link_libraries(gc_mgr status glog kvstore ${SYS_LIBS})

add_executable(migrate_test migrate_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(migrate_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(migrate_test index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
else()
	target_link_libraries(migrate_test commands index_mgr status glog test_util network gtest_main server_params ${SYS_LIBS})
	set_target_properties(migrate_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()

add_library(cluster_mgr cluster_manager.cpp)
target_link_libraries(cluster_mgr status lock glog utils_common ${SYS_LIBS})

add_executable(cluster_test cluster_test.cpp)
if(CMAKE_COMPILER_IS_GNUCC)
	target_link_libraries(cluster_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
	target_link_libraries(cluster_test cluster_mgr status utils_common test_util server network gtest_main ${SYS_LIBS})
	#set_target_properties(cluster_test PROPERTIES LINK_FLAGS "-static")
	set_target_properties(cluster_test PROPERTIES LINK_FLAGS "-static-libgcc -static-libstdc++")
else()
	target_link_libraries(cluster_test commands cluster_mgr status utils_common test_util server network gtest_main ${SYS_LIBS})
	set_target_properties(cluster_test PROPERTIES LINK_FLAGS "/WHOLEARCHIVE:commands")
endif()


