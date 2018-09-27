add_library(kvstore STATIC kvstore.cpp)
target_link_libraries(kvstore status stdc++fs glog)

add_library(pessimistic STATIC pessimistic.cpp)
target_link_libraries(pessimistic glog)

add_library(varint STATIC varint.cpp)

add_library(record STATIC record.cpp)
target_link_libraries(record varint status glog utils_common)

add_library(skiplist STATIC skiplist.cpp)
target_link_libraries(skiplist record varint status glog utils_common)

add_executable(varint_test varint_test.cpp)
target_link_libraries(varint_test varint status gtest_main)

add_executable(record_test record_test.cpp)
target_link_libraries(record_test record status gtest_main)

add_subdirectory(rocks)

add_library(catalog STATIC catalog.cpp)
target_link_libraries(catalog glog record kvstore)
