add_library(migrate STATIC migrate_manager.cpp migrate_sender.cpp migrate_receiver.cpp)
target_link_libraries(migrate status glog network catalog kvstore)
