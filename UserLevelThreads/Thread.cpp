#include "uthreads.h"
#include <signal.h>
#include <setjmp.h>
#include "Thread.h"

#ifdef __x86_64__
typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7
extern void thread_start();
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
            : "=g" (ret)
            : "0" (addr));
    return ret;
}
#else
#error Only 64-bit machines supported.
#endif

Thread::Thread() : tid(0), state(RUNNING) , quantum_count(0),sleep_quantums(0),active(1), env_initialized(1)
,manually_blocked(false)
{
    entry_point = nullptr;
    sigsetjmp(env[0], 1);
    sigemptyset(&env[0]->__saved_mask);
}

Thread::Thread(void (*entry_point_func)(), int tid) :
        tid(tid), state(READY), entry_point(entry_point_func),
        quantum_count(0), sleep_quantums(0), active(1), env_initialized(1),manually_blocked(false)
{
    if (sigsetjmp(env[0], 1) == 0)
    {
        address_t sp = (address_t)stack + STACK_SIZE - sizeof(address_t);
        address_t pc = (address_t)thread_start;

        // Setup stack pointer and program counter
        env[0]->__jmpbuf[JB_SP] = translate_address(sp);
        env[0]->__jmpbuf[JB_PC] = translate_address(pc);

        sigemptyset(&env[0]->__saved_mask);
    }
}

int Thread::getid()
{
    return tid;
}
Thread::~Thread(){

}
ThreadState Thread::getState()
{
    return state;
}

void Thread::setState(ThreadState new_state)
{
    state = new_state;
}

sigjmp_buf* Thread::get_env()
{
    return env;
}

void Thread::set_quantum_count(int new_total)
{
    quantum_count = new_total;
}

int Thread::get_quantum_count() const {
    return quantum_count;
}

char* Thread::getStack(){
    return stack;
}
