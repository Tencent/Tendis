#include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")

add_library(rocks_kvstore STATIC rocks_kvstore.cpp)
target_link_libraries(rocks_kvstore utils_common kvstore rocksdb record glog rt)

add_library(rocks_kvstore_debug STATIC rocks_kvstore.cpp)
target_compile_definitions(rocks_kvstore_debug PRIVATE WITH_SYNC_POINT)
target_link_libraries(rocks_kvstore_debug utils_common kvstore rocksdb record glog rt)

add_executable(rocks_kvstore_test rocks_kvstore_test.cpp)
target_link_libraries(rocks_kvstore_test utils_common kvstore rocksdb rocks_kvstore server_params record gtest_main stdc++fs jemalloc dl)
