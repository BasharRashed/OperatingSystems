cmake_minimum_required(VERSION 3.22)
project(ex2_OS)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(ex2_OS
        uthreads.cpp
        uthreads.h
        Thread.cpp
        test0_sanity.cpp
        test2_two_thread.cpp
        test1.cpp
        test2.cpp
        test3.cpp
        test4.cpp
        test5.cpp
        test6.cpp
        test7.cpp
        test8.cpp
        demo_itimer.c

)
