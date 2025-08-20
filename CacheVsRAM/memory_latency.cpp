// OS 2025 EX1

#include "memory_latency.h"

#include <iostream>
#include <climits>
#include "measure.h"
#include <cmath>
#define GALOIS_POLYNOMIAL ((1ULL << 63) | (1ULL << 62) | (1ULL << 60) | (1ULL << 59))
#define MAX_SIZE_ERR "max_size should be an int and at least 100. (max_size >=100)"
#define FACTOR_ERR "factor should be a number greater than 1. (factor > 1)"
#define REPEAT_ERR "repeat should be an an int and greater than 0. (repeat > 0)"

/**
 * Converts the struct timespec to time in nano-seconds.
 * @param t - the struct timespec to convert.
 * @return - the value of time in nano-seconds.
 */
uint64_t nanosectime(struct timespec t)
{
    // Your code here
    return t.tv_sec * 1000000000 + t.tv_nsec;
}

/**
 * Measures the average latency of accessing a given array in a sequential order.
 * @param repeat - the number of times to repeat the measurement for and average on.
 * @param arr - an allocated (not empty) array to preform measurement on.
 * @param arr_size - the length of the array arr.
 * @param zero - a variable containing zero in a way that the compiler doesn't "know" it in compilation time.
 * @return struct measurement containing the measurement with the following fields:
 *      double baseline - the average time (ns) taken to preform the measured operation without memory access.
 *      double access_time - the average time (ns) taken to preform the measured operation with memory access.
 *      uint64_t rnd - the variable used to randomly access the array, returned to prevent compiler optimizations.
 */
struct measurement measure_sequential_latency(uint64_t repeat, array_element_t *arr, uint64_t arr_size, uint64_t zero)
{
    repeat = arr_size > repeat ? arr_size : repeat; // Make sure repeat >= arr_size

    // Baseline measurement:
    struct timespec t0;
    timespec_get(&t0, TIME_UTC);
    register uint64_t rnd = 12345;
    for (register uint64_t i = 0; i < repeat; i++)
    {
        register uint64_t index = (i | (zero & rnd)) % arr_size;
        rnd ^= index & zero;

        rnd = (rnd >> 1) ^ ((0 - (rnd & 1)) & GALOIS_POLYNOMIAL); // Advance rnd pseudo-randomly (using Galois LFSR)
    }
    struct timespec t1;
    timespec_get(&t1, TIME_UTC);

    // Memory access measurement:
    struct timespec t2;
    timespec_get(&t2, TIME_UTC);
    rnd = (rnd & zero) ^ 12345;
    for (register uint64_t i = 0; i < repeat; i++)
    {
        register uint64_t index = (i | (zero & rnd)) % arr_size;
        rnd ^= arr[index] & zero;
        rnd = (rnd >> 1) ^ ((0 - (rnd & 1)) & GALOIS_POLYNOMIAL); // Advance rnd pseudo-randomly (using Galois LFSR)
    }
    struct timespec t3;
    timespec_get(&t3, TIME_UTC);

    // Calculate baseline and memory access times:
    double baseline_per_cycle = (double)(nanosectime(t1) - nanosectime(t0)) / (repeat);
    double memory_per_cycle = (double)(nanosectime(t3) - nanosectime(t2)) / (repeat);
    struct measurement result;

    result.baseline = baseline_per_cycle;
    result.access_time = memory_per_cycle;
    result.rnd = rnd;
    return result;
}

/**
 * Runs the logic of the memory_latency program. Measures the access latency for random and sequential memory access
 * patterns.
 * Usage: './memory_latency max_size factor repeat' where:
 *      - max_size - the maximum size in bytes of the array to measure access latency for.
 *      - factor - the factor in the geometric series representing the array sizes to check.
 *      - repeat - the number of times each measurement should be repeated for and averaged on.
 * The program will print output to stdout in the following format:
 *      mem_size_1,offset_1,offset_sequential_1
 *      mem_size_2,offset_2,offset_sequential_2
 *              ...
 *              ...
 *              ...
 */
int main(int argc, char *argv[])
{
    // zero==0, but the compiler doesn't know it. Use as the zero arg of measure_latency and measure_sequential_latency.
    struct timespec t_dummy;
    timespec_get(&t_dummy, TIME_UTC);
    const uint64_t zero = nanosectime(t_dummy) > 1000000000ull ? 0 : nanosectime(t_dummy);
    char *endptr;
    // check args numbers
    if (argc != 4)
    {
        return -1;
    }
    // check input validity
    uint64_t max_size = strtol(argv[1], &endptr, 10);
    if (*endptr != '\0' || endptr == argv[1] || max_size < 100)
    {
        std::cerr << MAX_SIZE_ERR << std::endl;
        return -1;
    }
    double factor = strtod(argv[2], &endptr);
    if (*endptr != '\0' || endptr == argv[2] || factor <= 1)
    {
        std::cerr << FACTOR_ERR << std::endl;
        return -1;
    }
    int repeat = strtol(argv[3], &endptr, 10);
    if (*endptr != '\0' || endptr == argv[3] || repeat <= 0)
    {
        std::cerr << REPEAT_ERR << std::endl;
        return -1;
    }
    uint64_t size = 100;
    // calcualting latency
    while (size < max_size)
    {
        uint64_t arr_size = (uint64_t)size / sizeof(array_element_t);
        array_element_t *arr = (array_element_t *)malloc(size);
        if (!arr)
        {
            return 1;
        }
        for (uint64_t i = 0; i < arr_size; i++)
        {
            arr[i] = i;
        }

        struct measurement result1 = measure_sequential_latency(repeat, arr, arr_size, zero);
        struct measurement result2 = measure_latency(repeat, arr, arr_size, zero);
        double offset1 = result1.access_time - result1.baseline;
        double offset2 = result2.access_time - result2.baseline;
        std::cout << size << "," << offset1 << "," << offset2 << std::endl;
        size = round(double(size) * factor);
        free(arr);
    }

    // Free allocated memory for the current array
    return 0;
    // Update size for the next iteration (geometric progression)
}