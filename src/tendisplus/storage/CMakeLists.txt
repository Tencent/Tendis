add_library(kvstore STATIC kvstore.cpp)
target_link_libraries(kvstore status)

add_library(varint STATIC varint.cpp)

add_executable(varint_test varint_test.cpp)
target_link_libraries(varint_test varint gtest_main)

add_subdirectory(rocks)
