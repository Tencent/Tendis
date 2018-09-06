add_library(repl_manager STATIC repl_manager.cpp mpov.cpp spov.cpp)
target_link_libraries(repl_manager status glog network catalog kvstore)
