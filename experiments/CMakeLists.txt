cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb")

# derecho_bw_test
add_executable(derecho_bw_test derecho_bw_test.cpp block_size.cpp aggregate_bandwidth.cpp initialize.cpp ../derecho_group.cpp)
target_link_libraries(derecho_bw_test rdmacm ibverbs boost_system rt pthread rdmc sst)
