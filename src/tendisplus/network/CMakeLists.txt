add_executable(helloDemo chat_server.cpp)
target_compile_options(helloDemo PUBLIC -DASIO_STANDALONE)
target_link_libraries(helloDemo -lpthread)

add_library(network network.cpp)
target_compile_options(network PUBLIC -DASIO_STANDALONE)
target_link_libraries(network -lpthread)
