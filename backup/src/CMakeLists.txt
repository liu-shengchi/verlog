SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/build)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
#SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
#SET (CMAKE_C_COMPILER "gcc")
#SET (CMAKE_CXX_COMPILER "g++")

set(CMAKE_CXX_COMPILER "/opt/rh/devtoolset-11/root/usr/bin/g++")
SET (CMAKE_BUILD_TYPE "Debug")
SET (CMAKE_CXX_FLAGS "-std=c++17 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O0 -g" CACHE INTERNAL "compiler options" FORCE)

list(APPEND EXTAR_INCLUDE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/util
                                 ${CMAKE_CURRENT_SOURCE_DIR}/benchmark 
                                 ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/tpcc
                                 ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/ycsb  
                                 ${CMAKE_CURRENT_SOURCE_DIR}/storage
                                 ${CMAKE_CURRENT_SOURCE_DIR}/executor
                                 ${CMAKE_CURRENT_SOURCE_DIR}/system 
                                 ${CMAKE_CURRENT_SOURCE_DIR}/transaction
                                 ${CMAKE_CURRENT_SOURCE_DIR}/access
                                 ${CMAKE_CURRENT_SOURCE_DIR}/log
                                 ${CMAKE_CURRENT_SOURCE_DIR}/log/log_strategy
                                 ${CMAKE_CURRENT_SOURCE_DIR}/statistic
                                 ${CMAKE_CURRENT_SOURCE_DIR}/communication)

file(GLOB_RECURSE SRC_FILES util/*.cpp
                            benchmark/*.cpp 
                            benchmark/tpcc/*.cpp 
                            benchmark/ycsb/*.cpp
                            executor/*.cpp
                            storage/*.cpp
                            transaction/*.cpp
                            access/*.cpp
                            system/*.cpp
                            log/*.cpp
                            log/log_strategy/*.cpp
                            statistic/*.cpp
                            communication/*.cpp)

ADD_EXECUTABLE(BACKUP ${SRC_FILES})

target_include_directories(BACKUP PUBLIC ${EXTAR_INCLUDE_HEADER})

# 动态链接库
target_link_libraries(BACKUP pthread)
target_link_libraries(BACKUP ibverbs)


# jemalloc 动态链接库
find_library(JEMALLOC_LIB jemalloc)
if(JEMALLOC_LIB)
    target_link_libraries(BACKUP ${JEMALLOC_LIB})
    message(STATUS "jemalloc library found at: ${JEMALLOC_LIB}")
else()
    message(FATAL_ERROR "jemalloc not found")
endif()
