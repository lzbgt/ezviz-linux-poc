cmake_minimum_required(VERSION 3.14)
project(ezviz)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-I/usr/local/include/c++/9.1.0 -Isrc/inc")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/EZServerOpenSDK/inc)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/EZServerOpenSDK/lib/linux64)

set(FRAMEWORKLIB crypto ssl curl protobuf expat amqpcpp dl uv cpp_redis tacopie PocoFoundation PocoNet PocoNetSSL)
set(LIB hpr hlog opensslwrap ys_net ezviz_streamclient ezserveropensdk)

#add_compile_options(
#  "-Wextra" "-fexceptions"
#  "$<$<CONFIG:DEBUG>:-O0;-g3;-ggdb>"
#)

link_libraries(${FRAMEWORKLIB} ${LIB})

#add_subdirectory(thirdparty/NFHTTP)


set(SOURCE_FILES src/main.cpp)
add_executable(ezviz ${SOURCE_FILES})
#target_link_libraries(ezviz NFHTTP)
#target_compile_definitions(ezviz PUBLIC __BYTE_ORDER = __LITTLE_ENDIAN)
