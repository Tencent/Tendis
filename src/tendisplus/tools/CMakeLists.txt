add_executable(ldb_tendis ldb_tendis.cpp)
target_link_libraries(ldb_tendis record rocksdb "-static -static-libgcc -static-libstdc++ -Wl,--no-as-needed -ldl -pthread -mno-avx -mno-avx2")
