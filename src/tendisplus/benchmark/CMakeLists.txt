add_executable(conflict_bench optimistic_transaction_bench.cpp)
target_include_directories(conflict_bench PUBLIC "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")
target_link_libraries(conflict_bench rocksdb rt)
