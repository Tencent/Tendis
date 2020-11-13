add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
SET(ROCKSDB_DIR ${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb)

include_directories(${ROCKSDB_DIR}/include) 
include_directories(${ROCKSDB_DIR})

add_executable(ldb_tendis ldb.cc ldb_cmd.cc ldb_tool.cc)

target_link_libraries(ldb_tendis utils_common kvstore record rocksdb glog ${SYS_LIBS})
