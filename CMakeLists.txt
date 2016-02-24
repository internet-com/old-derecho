cmake_minimum_required(VERSION 2.8)

PROJECT(verbs-mcast CXX)
SET(CMAKE_CXX_FLAGS "-std=c++1y -Wall -ggdb")

ADD_EXECUTABLE(main rdmc/rdmc.cpp rdmc/util.cpp main.cpp sst/tcp.cpp sst/verbs.cpp rdmc/group_send.cpp rdmc/verbs_helper.cpp rdmc/connection.cpp)
TARGET_LINK_LIBRARIES(main rdmacm ibverbs boost_system rt pthread)

add_custom_target(format clang-format-3.5 -i *.cpp *.h)
 
 
