include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")

add_library(rocks_kvstore STATIC rocks_kvstore.cpp)
target_link_libraries(rocks_kvstore status kvstore rocksdb)
