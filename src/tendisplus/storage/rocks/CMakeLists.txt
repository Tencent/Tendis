include(cmake/grpc.cmake)
# Proto file
get_filename_component(csa_proto "remote_compaction/protos/csa.proto" ABSOLUTE)
get_filename_component(csa_proto_path "${csa_proto}" PATH)

# Generated sources
set(csa_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/csa.pb.cc")
set(csa_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/csa.pb.h")
set(csa_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/csa.grpc.pb.cc")
set(csa_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/csa.grpc.pb.h")
add_custom_command(
        OUTPUT "${csa_proto_srcs}" "${csa_proto_hdrs}" "${csa_grpc_srcs}" "${csa_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${csa_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${csa_proto}"
        DEPENDS "${csa_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# csa_grpc_proto
add_library(csa_grpc_proto
        ${csa_grpc_srcs}
        ${csa_grpc_hdrs}
        ${csa_proto_srcs}
        ${csa_proto_hdrs})
target_link_libraries(csa_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})

# compaction_service_helper
#add_library(compaction_service_helper
#        "db/compaction/remote_compaction/job_distributor.h"
#        "db/compaction/remote_compaction/job_distributor.cc")
#target_link_libraries(compaction_service_helper
#        compaction_service_grpc_proto
#        ${_REFLECTION}
#        ${_GRPC_GRPCPP}
#        ${_PROTOBUF_LIBPROTOBUF})

# Targets compaction_service_(client|server)
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(LIBNFS libnfs)
endif()

if(NOT LIBNFS_FOUND)
    # 查找头文件
    find_path(LIBNFS_INCLUDE_DIR
        NAMES nfsc/libnfs.h
        PATHS
            /usr/include
            /usr/local/include
            /opt/libnfs/include
            $ENV{LIBNFS_ROOT}/include
        PATH_SUFFIXES nfs
    )
    
    # 查找库文件
    find_library(LIBNFS_LIBRARY
        NAMES nfs
        PATHS
            /usr/lib
            /usr/local/lib
            /usr/lib/x86_64-linux-gnu
            /usr/lib64
            /opt/libnfs/lib
            $ENV{LIBNFS_ROOT}/lib
    )
    
    # 检查是否找到
    if(LIBNFS_INCLUDE_DIR AND LIBNFS_LIBRARY)
        set(LIBNFS_FOUND TRUE)
        set(LIBNFS_INCLUDE_DIRS ${LIBNFS_INCLUDE_DIR})
        set(LIBNFS_LIBRARIES ${LIBNFS_LIBRARY})
        message(STATUS "Found libnfs: ${LIBNFS_LIBRARY}")
    else()
        message(FATAL_ERROR "libnfs not found! Please install libnfs-dev or set LIBNFS_ROOT environment variable")
    endif()
endif()
add_library(nfs_fs STATIC nfs_filesystem.cc nfs_file.cc nfs_filesystem_simple.cc shared_filesystem.cc)
# 添加头文件路径
target_include_directories(nfs_fs
    PUBLIC
        ${LIBNFS_INCLUDE_DIRS}
)

# 链接 libnfs
target_link_libraries(nfs_fs
    PUBLIC
        rocksdb
        ${LIBNFS_LIBRARIES}
)


foreach(_target csa_server)
    add_executable(${_target}
            "remote_compaction/${_target}.cc")
    add_dependencies(${_target} csa_grpc_proto)
    add_dependencies(${_target} nfs_fs)
    target_link_libraries(${_target}
            #          compaction_service_helper
            csa_grpc_proto
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF}
            rocksdb
            lz4_static
            nfs_fs
            )
endforeach()

# message(STATUS "askjdakshakjsdgajdhajgjdsh")
# add_executable(remote_test remote_test.cpp remote_util.h remote_util.cc)#ifndef NDEBUG
# target_compile_definitions(remote_test PUBLIC -DNDEBUG)
# target_link_libraries(remote_test rocksdb lz4_static)

# add_library(rocks_kvstore STATIC rocks_kvstore.cpp rocks_kvttlcompactfilter.cpp)
# target_link_libraries(rocks_kvstore utils_common kvstore rocksdb record glog ${SYS_LIBS} snappy lz4_static)

add_library(rocks_kvstore STATIC rocks_kvstore.cpp rocks_kvttlcompactfilter.cpp compaction_service.cc)
target_link_libraries(rocks_kvstore utils_common kvstore rocksdb record glog ${SYS_LIBS} snappy lz4_static  csa_grpc_proto 
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF}
            nfs_fs
            )

add_library(rocks_kvstore_for_test STATIC rocks_kvstore.cpp rocks_kvttlcompactfilter.cpp)
target_compile_definitions(rocks_kvstore_for_test PRIVATE -DNO_VERSIONEP)
target_link_libraries(rocks_kvstore_for_test utils_common kvstore rocksdb record glog ${SYS_LIBS} snappy lz4_static)

if(TENDIS_WITH_UNIT_TESTS)
add_executable(rocks_kvstore_test rocks_kvstore_test.cpp)
target_link_libraries(rocks_kvstore_test utils_common kvstore rocksdb
	session_ctx session lock server -Wl,--whole-archive commands -Wl,--no-whole-archive
    rocks_kvstore server_params record gtest_main ${SYS_LIBS} snappy lz4_static)
endif()
