add_library(repl_manager STATIC repl_manager.cpp mpov.cpp spov.cpp repl_util.cpp)
target_link_libraries(repl_manager status glog network catalog kvstore)

add_executable(binlog_tool binlog_tool.cpp)
target_link_libraries(binlog_tool glog kvstore rocks_kvstore varint utils_common)
#set_target_properties(binlog_tool PROPERTIES LINK_FLAGS "-static") # -static-libasan
set_target_properties(binlog_tool PROPERTIES LINK_FLAGS "-static-libgcc -static-libstdc++")
