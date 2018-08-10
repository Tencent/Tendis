
#-DASIO_STANDALONE

add_executable (helloDemo chat_server.cpp)
target_include_directories (helloDemo PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../thirdparty/asio-asio-1-12-0/asio/include)
target_compile_options(helloDemo PUBLIC -DASIO_STANDALONE)
target_link_libraries(helloDemo -lpthread)
