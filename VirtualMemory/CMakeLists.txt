cmake_minimum_required(VERSION 3.22)
project(ex4Ver2)

set(CMAKE_CXX_STANDARD 11)

add_executable(ex4Ver2
        PhysicalMemory.cpp
        SimpleTest.cpp
        VirtualMemory.cpp
        MemoryConstants_test1.h
        MemoryConstants_test2.h
        test1_write_read_all_virtual_memory.cpp
        test2_write_one_page_twice_and_read.cpp)
