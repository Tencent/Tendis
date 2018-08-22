add_library(commands STATIC command.cpp get.cpp set.cpp)
target_link_libraries(commands status network)

add_executable(commmand_test command_test.cpp)

# command uses global variables to self-regist, linking flag whole-archive is required.
target_link_libraries(commmand_test -Wl,--whole-archive commands -Wl,--no-whole-archive)
target_link_libraries(commmand_test commands status network server_params stdc++fs gtest_main)
