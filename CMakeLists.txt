cmake_minimum_required(VERSION 2.8)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")
add_subdirectory(rdmc EXCLUDE_FROM_ALL)
add_subdirectory(sst EXCLUDE_FROM_ALL)
add_subdirectory(experiments)

add_executable(derecho main.cpp)
add_executable(conntest connection_test.cpp)
add_executable(gms_test gms_test.cpp managed_group.cpp view.cpp derecho_group.cpp view_utils.cpp)
target_link_libraries(derecho rdmacm ibverbs boost_system rt pthread atomic rdmc sst)
target_link_libraries(conntest rdmc sst)
target_link_libraries(gms_test rdmacm ibverbs boost_system rt pthread atomic rdmc sst)
