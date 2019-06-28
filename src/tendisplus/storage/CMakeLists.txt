add_library(kvstore STATIC kvstore.cpp)
target_link_libraries(kvstore status ${STDFS_LIB} glog)

add_library(pessimistic STATIC pessimistic.cpp)
target_link_libraries(pessimistic glog)

add_library(varint STATIC varint.cpp)
target_link_libraries(varint glog)

add_library(record STATIC record.cpp repllog.cpp)
target_link_libraries(record varint status glog utils_common)

add_library(skiplist STATIC skiplist.cpp)
target_link_libraries(skiplist record varint status glog utils_common)

add_executable(varint_test varint_test.cpp)
target_link_libraries(varint_test varint status glog gtest_main ${SYS_LIBS})

add_executable(record_test record_test.cpp)
target_link_libraries(record_test record status gtest_main ${SYS_LIBS})

add_executable(skiplist_test skiplist_test.cpp)
target_link_libraries(skiplist_test skiplist rocks_kvstore_for_test server_params status gtest_main ${SYS_LIBS})

add_subdirectory(rocks)

add_library(catalog STATIC catalog.cpp)
target_link_libraries(catalog glog record kvstore)
