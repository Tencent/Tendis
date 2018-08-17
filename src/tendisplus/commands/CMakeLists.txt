add_library(commands STATIC command.cpp get.cpp)
target_link_libraries(commands status network)
