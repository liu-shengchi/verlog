CMAKE_MINIMUM_REQUIRED(VERSION 2.8.12)
#SET (CMAKE_C_COMPILER "gcc")
#SET (CMAKE_CXX_COMPILER "g++")
#SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
#SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
#SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

# 添加Boost子目录
# ADD_SUBDIRECTORY(boost)

PROJECT(PRIMARY)
ADD_SUBDIRECTORY(src build)

# 链接Boost库
# target_link_libraries(PRIMARY PUBLIC boost)
