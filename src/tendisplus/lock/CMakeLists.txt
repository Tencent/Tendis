add_subdirectory(mgl)

add_library(lock lock.cpp)
target_link_libraries(lock mgl)

add_executable(lock_test lock_test.cpp)
target_link_libraries(lock_test lock session session_ctx server gtest_main ${SYS_LIBS})
