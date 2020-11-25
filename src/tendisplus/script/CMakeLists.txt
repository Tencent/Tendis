add_library(script STATIC script_manager.cpp sha1.c)
target_link_libraries(script glog lua)

add_executable(script_test script_test.cpp)
target_link_libraries(script_test script gtest_main ${SYS_LIBS})
