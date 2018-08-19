include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")

add_library(rocks_kvstore STATIC rocks_kvstore.cpp)
target_link_libraries(rocks_kvstore status kvstore rocksdb glog)

add_executable(rocks_kvstore_test rocks_kvstore_test.cpp)
target_link_libraries(rocks_kvstore_test status kvstore rocksdb rocks_kvstore server_params gtest_main stdc++fs rt)
