add_library(mgl mgl.cpp mgl_mgr.cpp)
target_link_libraries(mgl glog)

add_executable(mgl_test mgl_test.cpp)
target_link_libraries(mgl_test mgl gtest_main utils_common ${SYS_LIBS})
