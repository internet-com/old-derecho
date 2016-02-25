cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb")
add_subdirectory(rdmc)
add_subdirectory(sst)

add_executable(derecho main.cpp)
target_link_libraries(derecho rdmacm ibverbs boost_system rt pthread rdmc sst)
