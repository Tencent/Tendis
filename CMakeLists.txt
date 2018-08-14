cmake_minimum_required (VERSION 2.8.0)
project (TENDIS_PLUS)

# configure compiler requirement
message(${CMAKE_CXX_COMPILER})
if(CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.5)
        message(FATAL_ERROR "g++ version ${CMAKE_CXX_COMPILER_VERSION}, Require at least gcc-4.8, \
        you may remove CMakeCache.txt and run export CXX=/path/to/your/prefered/cxx_compiler before run cmake \
        to solve this problem")
    endif()
else()
    message(FATAL_ERROR "gcc is required")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")

include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/googletest-release-1.8.0/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/asio-asio-1-12-0/asio/include")
include_directories("${PROJECT_SOURCE_DIR}/src/thirdparty/glog-0.3.5")

# thirdparty code
add_subdirectory(src/thirdparty/googletest-release-1.8.0)
add_subdirectory(src/thirdparty//glog-0.3.5)

add_subdirectory(src/tendisplus/network)
add_subdirectory(src/tendisplus/utils)
add_subdirectory(src/tendisplus/server)
