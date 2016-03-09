cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb")
add_executable(test_group_interface test_group_interface.cpp)
target_link_libraries(test_group_interface rdmacm ibverbs boost_system rt pthread rdmc sst)
