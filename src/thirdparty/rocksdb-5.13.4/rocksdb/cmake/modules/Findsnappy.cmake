# - Find Snappy
# Find the snappy compression library and includes
#
# SNAPPY_INCLUDE_DIR - where to find snappy.h, etc.
# SNAPPY_LIBRARIES - List of libraries when using snappy.
# SNAPPY_FOUND - True if snappy found.

#find_path(SNAPPY_INCLUDE_DIR
#  NAMES snappy.h
#  HINTS ${SNAPPY_ROOT_DIR}/include)

#set(SNAPPY_INCLUDE_DIR "/data/home/vinchen/git/tendisplus/libs/snappy/")
set(SNAPPY_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/thirdparty/include/snappy/")

#find_library(SNAPPY_LIBRARIES
#  NAMES snappy
#  HINTS ${SNAPPY_ROOT_DIR}/lib)

set(SNAPPY_LIBRARIES "${CMAKE_BINARY_DIR}/lib/libsnappy.a")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(snappy DEFAULT_MSG SNAPPY_LIBRARIES SNAPPY_INCLUDE_DIR)

mark_as_advanced(
  SNAPPY_LIBRARIES
  SNAPPY_INCLUDE_DIR)
