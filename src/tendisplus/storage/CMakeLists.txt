add_library(kvstore STATIC kvstore.cpp)
target_link_libraries(kvstore status)

add_subdirectory(rocks)
