cmake_minimum_required (VERSION 3.13.0)
project (TENDIS_PLUS)

execute_process(COMMAND ./mkreleasehdr.sh WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

option(TENDIS_DEBUG "define TENDIS_DEBUG" ON)
if(TENDIS_DEBUG)
    add_definitions(-DTENDIS_DEBUG)
endif()

# configure compiler requirement
message(${CMAKE_CXX_COMPILER})
if(CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.5)
        message(FATAL_ERROR "g++ version ${CMAKE_CXX_COMPILER_VERSION}, Require at least gcc-5.5, \
        you may remove CMakeCache.txt and run export CXX=/path/to/your/prefered/cxx_compiler before run cmake \
        to solve this problem")
    endif()
elseif(MSVC AND MSVC_VERSION GREATER_EQUAL 1900)
	message("using MSVC ${MSVC_VERSION}")
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
    message(FATAL_ERROR "gcc or vs2015(update 2)+ is required")
endif()

if(CMAKE_COMPILER_IS_GNUCC)
	set(STDFS_LIB "stdc++fs")
    option(NEW_ROCKSDB "new rocksdb version" ON)
    if(NEW_ROCKSDB)
        message("using rocksdb-6.2.2")
        set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb")
    else()
        message("using rocksdb-5.3.14")
        set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4")
    endif()
else()
	set(STDFS_LIB "")
	set(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-6.0.0/rocksdb")
endif()

set(CMAKE_MODULE_PATH "${ROCKSDB_DIR}/cmake/modules/")


set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(CMAKE_COMPILER_IS_GNUCC)
	option(ENABLE_JEMALLOC "enable jemalloc" ON)
	if(ENABLE_JEMALLOC)
		set(JEMALLOC_ROOT_DIR "${PROJECT_SOURCE_DIR}/src/thirdparty/jemalloc")
		option(WITH_JEMALLOC "enable jemalloc of rocksdb" ON)

		add_custom_command(OUTPUT "${JEMALLOC_ROOT_DIR}/include/jemalloc/jemalloc.h"
				COMMAND ./autogen.sh
				WORKING_DIRECTORY ${JEMALLOC_ROOT_DIR})
		add_custom_command(OUTPUT "${JEMALLOC_ROOT_DIR}/lib/libjemalloc.a"
                "${JEMALLOC_ROOT_DIR}/lib/libjemalloc.so"
				COMMAND ${CMAKE_MAKE_PROGRAM}
				DEPENDS "${JEMALLOC_ROOT_DIR}/include/jemalloc/jemalloc.h"
				WORKING_DIRECTORY ${JEMALLOC_ROOT_DIR}
				COMMENT "Making external library jemalloc")
		add_custom_target(build_jemalloc
				DEPENDS "${JEMALLOC_ROOT_DIR}/lib/libjemalloc.a")

		add_library(jemalloc_SHARED SHARED IMPORTED)
		add_library(jemalloc STATIC IMPORTED)
		set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_ROOT_DIR}/lib/libjemalloc_pic.a")
		set_target_properties(jemalloc_SHARED PROPERTIES IMPORTED_LOCATION "${JEMALLOC_ROOT_DIR}/lib/libjemalloc.so")

		list(APPEND SYS_LIBS jemalloc)
		list(APPEND THIRDPARTY_LIBS jemalloc)
		add_dependencies(jemalloc build_jemalloc)
		add_dependencies(jemalloc_SHARED build_jemalloc)
	endif()

	list(APPEND SYS_LIBS "dl" "pthread" "rt")
	# thirdparty code
	# do not set unnecessary flags before thirdparty dirs
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -fno-omit-frame-pointer ")
endif()


# TODO(vinchen): in the future it should disable sync_ponit in production
add_definitions(-DWITH_SYNC_POINT)
list(APPEND SYS_LIBS sync_point)
list(APPEND SYS_LIBS snappy)

add_subdirectory(src/thirdparty/gflag)
add_subdirectory(src/thirdparty/snappy)
target_compile_options(snappy PRIVATE -fPIC)
add_subdirectory(src/thirdparty/lz4/contrib/cmake_unofficial)
add_subdirectory(src/thirdparty/googletest)
add_subdirectory(src/thirdparty/glog)

set(WITH_TESTS OFF)
set(WITH_BENCHMARK_TOOLS OFF)
set(WITH_TOOLS OFF)
set(WITH_JNI OFF)
set(WITH_GFLAGS OFF)

add_definitions(-DSNAPPY)
set(snappy_FOUND TRUE)
set(snappy_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/thirdparty/include/snappy/)
set(snappy_LIBRARIES ${CMAKE_BINARY_DIR}/lib/libsnappy.a)
list(APPEND THIRDPARTY_LIBS snappy)
set(LZ4_FOUND TRUE)
set(LZ4_LIBRARIES "${CMAKE_BINARY_DIR}/lib/liblz4.a")
include_directories("src/thirdparty/lz4/lib/")
list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARIES})
add_definitions(-DLZ4)

option(RAPIDJSON_BUILD_TESTS "" OFF)
add_subdirectory(src/thirdparty/rapidjson)

add_subdirectory(${ROCKSDB_DIR}/rocksdb)
if(CMAKE_COMPILER_IS_GNUCC)
set(WARN_C_CXX
"-Wall \
-Wextra \
-Wno-unused-but-set-variable \
-Wno-unused-parameter \
-Wno-unused-local-typedefs \
-Wno-missing-field-initializers \
-Wno-implicit-fallthrough \
-Wno-float-equal \
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

# gflags

include_directories("src/thirdparty/include")
include_directories("src/thirdparty/include/snappy/")
include_directories("src/thirdparty/lz4/lib/")
else()
include_directories("${PROJECT_SOURCE_DIR}/src/tendisplus/include")
#include_directories("${PROJECT_SOURCE_DIR}/src/tendisplus/include/snappy")
#include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/lz4/lib/")

# gflags
include_directories("src/thirdparty/include/Windows/")
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 ${WARN_C_CXX} ${WARN_CXX_ONLY}")

add_definitions(-DASIO_STANDALONE)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

message("${CMAKE_BUILD_TYPE}")
message("${CMAKE_CXX_FLAGS_RELEASE}")
include_directories("${PROJECT_SOURCE_DIR}/src")

include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/googletest/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/asio/asio/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog/src")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rapidjson/include")
include_directories(${ROCKSDB_DIR})
include_directories(${ROCKSDB_DIR}/include)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
add_subdirectory(src/tendisplus/network)
add_subdirectory(src/tendisplus/utils)
add_subdirectory(src/tendisplus/server)
add_subdirectory(src/tendisplus/commands)
add_subdirectory(src/tendisplus/storage)
add_subdirectory(src/tendisplus/replication)
add_subdirectory(src/tendisplus/cluster)
add_subdirectory(src/tendisplus/lock)
add_subdirectory(src/tendisplus/tools)

