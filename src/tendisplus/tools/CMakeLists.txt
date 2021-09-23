add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_SUPPORT_THREAD_LOCAL)

set(patchCommand "cd ${PROJECT_SOURCE_DIR}/src/tendisplus/tools\;cp ${ROCKSDB_DIR}/rocksdb/tools/ldb* .\; git am -s 0001-fix-ldb-tools.patch")
message("RocksDB ldb tools git patch, we apply add-property-rocksdb.levelstatsex-in-rocksdb ${patchCommand}")
execute_process(COMMAND bash "-c" ${patchCommand})

add_executable(ldb_tendis ldb.cc ldb_cmd.cc ldb_cmd_tendis ldb_tool.cc)

target_link_libraries(ldb_tendis utils_common kvstore record rocksdb glog ${SYS_LIBS} "-static-libgcc -static-libstdc++ -Wl,--no-as-needed -ldl -pthread -mno-avx -mno-avx2")
