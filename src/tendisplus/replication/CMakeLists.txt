add_library(repl_manager STATIC repl_manager.cpp mpov.cpp spov.cpp)
target_link_libraries(repl_manager status glog network catalog kvstore)

add_executable(binlog_tool binlog_tool.cpp)
target_link_libraries(binlog_tool glog param_manager kvstore rocks_kvstore varint base64)
