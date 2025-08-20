//#include "VirtualMemory.h"
//#include "PhysicalMemory.h"
//#include <math.h>
//
//#include <cstdio>
//#include <cassert>
//#include <iostream>
//
//int main(int argc, char **argv) {
//    VMinitialize();
//    for (uint64_t i = 0; i < VIRTUAL_MEMORY_SIZE; ++i) {
//        VMwrite(i, i);
//    }
//
//    for (uint64_t i = 0; i < VIRTUAL_MEMORY_SIZE; ++i) {
//        word_t value;
//        VMread(i, &value);
//        assert(uint64_t(value) == i);
//    }
//
//    printf("success\n");
//
//    return 0;
//}
