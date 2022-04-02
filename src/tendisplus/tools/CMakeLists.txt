# When update RocksDB Version, should update fix-ldb-tools.patch

add_executable(ldb_tendis ldb.cc ldb_cmd.cc ldb_cmd_tendis.cc ldb_tool.cc)

target_link_libraries(ldb_tendis utils_common kvstore record rocksdb glog ${SYS_LIBS} "-static-libgcc -static-libstdc++ -Wl,--no-as-needed -ldl -pthread -mno-avx -mno-avx2")
