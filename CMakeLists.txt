cmake_minimum_required(VERSION 2.8.1)

project(derecho CXX)
set(CMAKE_CXX_FLAGS "-std=c++14 -Wall -ggdb -gdwarf-3")
add_subdirectory(rdmc EXCLUDE_FROM_ALL)
add_subdirectory(sst EXCLUDE_FROM_ALL)
add_subdirectory(experiments)
add_custom_target(
	mutils
	COMMAND make
	WORKING_DIRECTORY ./mutils
)
include_directories(mutils)
add_custom_target(
	mutils_serialization
	COMMAND make
	WORKING_DIRECTORY ./serialization
)
include_directories(serialization)
find_library(SERIALIZATION_LIBRARY mutils-serialization PATHS ./serialization)

add_library(derecho SHARED managed_group.cpp view.cpp view_utils.cpp derecho_row.cpp logger.cpp filewriter.cpp)
target_link_libraries(derecho rdmacm ibverbs rt pthread atomic rdmc sst ${SERIALIZATION_LIBRARY})
add_dependencies(derecho mutils_serialization)

add_executable(main main.cpp)
target_link_libraries(main derecho)

add_executable(conntest connection_test.cpp)
target_link_libraries(conntest rdmc sst)

add_executable(gms_test gms_test.cpp experiments/block_size.cpp)
target_link_libraries(gms_test derecho)

add_executable(gms_test2 gms_test2.cpp experiments/block_size.cpp)
target_link_libraries(gms_test2 derecho)

add_custom_target(format_derecho clang-format-3.6 -i *.cpp *.h)
