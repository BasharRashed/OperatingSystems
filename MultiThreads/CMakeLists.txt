cmake_minimum_required(VERSION 3.22)
project(Ex3OS)

set(CMAKE_CXX_STANDARD 14)

add_executable(Ex3OS
        Barrier.cpp
        Barrier.h
        MapReduceClient.h
        MapReduceFramework.cpp
        MapReduceFramework.h
       test4-1_thread_4_process.cpp)
