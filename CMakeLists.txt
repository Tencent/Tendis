cmake_minimum_required (VERSION 2.8.0)
project (TENDIS_PLUS)

message("TODO(deyukong): currently libsnappy depends on the system provided, integrate it into thirdparty")
message("TODO(deyukong): currently jemalloc can only be used by LD_PRELOAD, integrate it into thirdparty \
 and compile internally")

# configure compiler requirement
message(${CMAKE_CXX_COMPILER})
if(CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.5)
        message(FATAL_ERROR "g++ version ${CMAKE_CXX_COMPILER_VERSION}, Require at least gcc-5.5, \
        you may remove CMakeCache.txt and run export CXX=/path/to/your/prefered/cxx_compiler before run cmake \
        to solve this problem")
    endif()
elseif(MSVC)
	message("using MSVC")
	# TODO: check msvc version
	# for rocksdb, disable warning as errors
	add_compile_options(/WX-) 

	# for glog
	add_definitions(-DGLOG_NO_ABBREVIATED_SEVERITIES)

	# for asio 
	#add_definitions(-DWIN32_LEAN_AND_MEAN)

	# disable warnings 
	#add_compile_options(/wC4099)
else()
    message(FATAL_ERROR "gcc or vs2015+ is required")
endif()

if(CMAKE_COMPILER_IS_GNUCC)
	set(RT_LIB "rt")
	set(PTHREAD_LIB "pthread")
	set(STDFS_LIB "stdc++fs")
	set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4")
	set(GOOGLE_TEST_DIR "googletest-release-1.8.0")
else()
	set(RT_LIB "")
	set(PTHREAD_LIB "")
	set(STDFS_LIB "")
	set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-6.0.0")
	set(GOOGLE_TEST_DIR "googletest-release-1.8.0.1")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)



if(CMAKE_COMPILER_IS_GNUCC)
	option(ENABLE_JEMALLOC "enable jemalloc" ON)
	list(APPEND SYS_LIBS "dl" "pthread" "rt")
	if(ENABLE_JEMALLOC)
		list(APPEND SYS_LIBS jemalloc)
	endif()

	# thirdparty code
	# do not set unnecessary flags before thirdparty dirs
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g")
endif()

# TODO(vinchen): in the future it should disable sync_ponit in production
add_definitions(-DWITH_SYNC_POINT)
list(APPEND SYS_LIBS sync_point)

add_subdirectory(src/thirdparty/gflags)
add_subdirectory(src/thirdparty/${GOOGLE_TEST_DIR})
add_subdirectory(src/thirdparty/glog-0.3.5)
add_subdirectory(${ROCKSDB_DIR}/rocksdb)
add_subdirectory(src/thirdparty/rapidjson-1.1.0)

if(CMAKE_COMPILER_IS_GNUCC)
set(WARN_C_CXX
"-Wall \
-Wextra \
-Wno-unused-but-set-variable \
-Wno-unused-parameter \
-Wno-unused-local-typedefs \
-Wno-missing-field-initializers \
-Wendif-labels \
-Wfloat-equal \
-Wformat=2 \
-Wframe-larger-than=69632 \
-Wmissing-include-dirs \
-Wpointer-arith \
-Wwrite-strings \
-Werror=char-subscripts \
-Werror=comments \
-Werror=conversion-null \
-Werror=empty-body \
-Werror=endif-labels \
-Werror=format \
-Werror=format-nonliteral \
-Werror=missing-include-dirs \
-Werror=overflow \
-Werror=parentheses \
-Werror=reorder \
-Werror=return-type \
-Werror=sequence-point \
-Werror=sign-compare \
-Werror=switch \
-Werror=type-limits \
-Werror=uninitialized \
-Werror=unused-function \
-Werror=unused-label \
-Werror=unused-result \
-Werror=unused-value \
-Werror=unused-variable \
-Werror=write-strings")

set(WARN_CXX_ONLY
"-Wno-invalid-offsetof \
-Wnon-virtual-dtor \
-Woverloaded-virtual \
-Wvla \
-Werror=non-virtual-dtor \
-Werror=overloaded-virtual \
-Werror=vla")

set(WARN_C_ONLY
"-Werror-implicit-function-declaration")
else()
include_directories("${PROJECT_SOURCE_DIR}/src/tendisplus/include")

# gflags
include_directories("src/thirdparty/include")
#include_directories("D:/git/tendis/rocksdb/snappy-1.1.7")
#include_directories("D:/git/tendis/rocksdb/snappy-1.1.7/bld")
#include_directories("D:/git/tendis/rocksdb/zstd-1.3.7/lib")
#include_directories("D:/git/tendis/rocksdb/zstd-1.3.7/lib/dictBuilder")
#include_directories("D:/git/tendis/rocksdb/lz4-1.7.5/lib")
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 ${WARN_C_CXX} ${WARN_CXX_ONLY}")

add_definitions(-DASIO_STANDALONE)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

message("${CMAKE_BUILD_TYPE}")
message("${CMAKE_CXX_FLAGS_RELEASE}")
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${ROCKSDB_DIR}/rocksdb/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/${GOOGLE_TEST_DIR}/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/asio-asio-1-12-0/asio/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog-0.3.5")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog-0.3.5/src")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rapidjson-1.1.0/include")

add_subdirectory(src/tendisplus/network)
add_subdirectory(src/tendisplus/utils)
add_subdirectory(src/tendisplus/server)
add_subdirectory(src/tendisplus/benchmark)
add_subdirectory(src/tendisplus/commands)
add_subdirectory(src/tendisplus/storage)
add_subdirectory(src/tendisplus/replication)
add_subdirectory(src/tendisplus/lock)
