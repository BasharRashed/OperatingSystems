#include "VirtualMemory.h"
#include <math.h>
#include "PhysicalMemory.h"

#include <cstdio>
#include <cassert>
#include <iostream>

int main(int argc, char **argv) {
    VMinitialize();
    for (uint64_t i = 0; i < (2 * PAGE_SIZE); ++i) {
        VMwrite(i%PAGE_SIZE, i);
    }
    for (uint64_t i = 0; i < (PAGE_SIZE); ++i) {
        word_t value;
        VMread(i, &value);
        assert(uint64_t(value) == i+PAGE_SIZE);
    }
    printRam();
    printEvictionCounter();

    return 0;
}


