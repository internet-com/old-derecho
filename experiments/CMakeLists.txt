cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb")

# test_group_interface
add_executable(test_group_interface test_group_interface.cpp)
target_link_libraries(test_group_interface rdmacm ibverbs boost_system rt pthread rdmc sst)

# single_sender_one_message
add_executable(single_sender_one_message single_sender_one_message.cpp)
target_link_libraries(single_sender_one_message rdmacm ibverbs boost_system rt pthread rdmc sst)

# single_sender_multiple_messages
add_executable(single_sender_multiple_messages single_sender_multiple_messages.cpp)
target_link_libraries(single_sender_multiple_messages rdmacm ibverbs boost_system rt pthread rdmc sst)

# multiple_senders_multiple_messages
add_executable(multiple_senders_multiple_messages multiple_senders_multiple_messages.cpp)
target_link_libraries(multiple_senders_multiple_messages rdmacm ibverbs boost_system rt pthread rdmc sst)

# bandwidth_test
add_executable(bandwidth_test bandwidth_test.cpp)
target_link_libraries(bandwidth_test rdmacm ibverbs boost_system rt pthread rdmc sst)
