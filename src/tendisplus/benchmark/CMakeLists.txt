add_executable(conflict_bench optimistic_transaction_bench.cpp)
target_include_directories(conflict_bench PUBLIC "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")
target_link_libraries(conflict_bench rocksdb glog ${SYS_LIBS})

add_executable(multi_bench multi_benchmark.cpp)
target_include_directories(multi_bench PUBLIC "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")
target_link_libraries(multi_bench rocksdb ${SYS_LIBS})
