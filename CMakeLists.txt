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
else()
    message(FATAL_ERROR "gcc is required")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# thirdparty code
# do not set unnecessary flags before thirdparty dirs
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g")
add_subdirectory(src/thirdparty/googletest-release-1.8.0)
add_subdirectory(src/thirdparty/glog-0.3.5)
add_subdirectory(src/thirdparty/rocksdb-5.13.4/rocksdb)
add_subdirectory(src/thirdparty/rapidjson-1.1.0)

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

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 ${WARN_C_CXX} ${WARN_CXX_ONLY}")

add_definitions(-DASIO_STANDALONE)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rocksdb-5.13.4/rocksdb/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/googletest-release-1.8.0/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/asio-asio-1-12-0/asio/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog-0.3.5")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/rapidjson-1.1.0/include") 

add_subdirectory(src/tendisplus/network)
add_subdirectory(src/tendisplus/utils)
add_subdirectory(src/tendisplus/server)
add_subdirectory(src/tendisplus/benchmark)
add_subdirectory(src/tendisplus/commands)
add_subdirectory(src/tendisplus/storage)
add_subdirectory(src/tendisplus/replication)
