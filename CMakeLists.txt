cmake_minimum_required(VERSION 2.8)
project(ezviz)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/EZServerOpenSDK/inc)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/EZServerOpenSDK/lib/linux64)

set(FRAMEWORKLIB crypto ssl curl protobuf expat)
set(LIB hpr hlog opensslwrap ys_net ezviz_streamclient ezserveropensdk)
link_libraries(${FRAMEWORKLIB} ${LIB})

set(SOURCE_FILES src/main.cpp)
add_executable(ezviz ${SOURCE_FILES})
