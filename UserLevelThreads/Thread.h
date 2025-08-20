//
// Created by bashar on 4/27/2025.
//
#include "uthreads.h"
#include <setjmp.h>

#ifndef EX2_OS_THREAD_H
#define EX2_OS_THREAD_H

#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr);

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5
/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
		"rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}

#endif

typedef enum {
    READY = 1,RUNNING, BLOCKED
}ThreadState;

class Thread {
private:
    int tid;
    ThreadState state;
    void (*entry_point) (void);
    sigjmp_buf env[1];
    char stack[STACK_SIZE];
public:
    int quantum_count;
    int sleep_quantums;
    int active;
    int env_initialized;
    bool manually_blocked;

    //empty constructor
    Thread();
    Thread(void (*entry_point)(void), int tid);
    virtual ~Thread();
    int getid ();

    ThreadState getState();
    void setState(ThreadState new_state);
    sigjmp_buf* get_env();
    void set_quantum_count(int new_total);
    char* getStack();
    void (*get_entry_point() const)(){
        return entry_point;
    }
    int get_quantum_count() const;
};


#endif //EX2_OS_THREAD_H
