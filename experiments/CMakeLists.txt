cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb")

# test_group_interface
add_executable(test_group_interface test_group_interface.cpp ../derecho_group.cpp)
target_link_libraries(test_group_interface rdmacm ibverbs boost_system rt pthread rdmc sst)

# single_sender_one_message
add_executable(single_sender_one_message single_sender_one_message.cpp ../derecho_group.cpp)
target_link_libraries(single_sender_one_message rdmacm ibverbs boost_system rt pthread rdmc sst)

# single_sender_multiple_messages
add_executable(single_sender_multiple_messages single_sender_multiple_messages.cpp ../derecho_group.cpp)
target_link_libraries(single_sender_multiple_messages rdmacm ibverbs boost_system rt pthread rdmc sst)

# multiple_senders_multiple_messages
add_executable(multiple_senders_multiple_messages multiple_senders_multiple_messages.cpp ../derecho_group.cpp)
target_link_libraries(multiple_senders_multiple_messages rdmacm ibverbs boost_system rt pthread rdmc sst)

# derecho_bw_test
add_executable(derecho_bw_test derecho_bw_test.cpp ../derecho_group.cpp)
target_link_libraries(derecho_bw_test rdmacm ibverbs boost_system rt pthread rdmc sst)

# rdmc_bw_test
add_executable(rdmc_bw_test rdmc_bw_test.cpp ../derecho_group.cpp)
target_link_libraries(rdmc_bw_test rdmacm ibverbs boost_system rt pthread rdmc sst)

# window_size_test
add_executable(window_size_test window_size_test.cpp block_size.cpp ../derecho_group.cpp)
target_link_libraries(window_size_test rdmacm ibverbs boost_system rt pthread rdmc sst)

# write_to_ssd
add_executable(write_to_ssd write_to_ssd.cpp block_size.cpp ../derecho_group.cpp)
target_link_libraries(write_to_ssd rdmacm ibverbs boost_system rt pthread rdmc sst)
