#include "uthreads.h"
#include "Thread.h"
#include <iostream>
#include <queue>
#include <signal.h>
#include <setjmp.h>
#include <sys/time.h>
#include <cstdlib>

#define FAIL -1
#define SUCCESS 0
#define QUANTUM_USECS_ERR "thread library error: quantum_usecs must be positive"
#define SIGACTION_ERR "system error: sigaction failed"
#define TIMER_ERR "system error: setitimer failed"
#define INVALID_ENTRY_PTR "thread library error: invalid function pointer"
#define UNAVAILABLE_THREAD_ERR "thread library error: no available thread ID"
#define TID_VALIDATION_ERR "thread library error: invalid thread ID"
#define MAIN_THREAD_BLOCK_ERR "thread library error: cannot block main thread"
#define NUM_OF_QUANTUMS_ERR "thread library error: invalid number of quantums to sleep"
#define MAIN_THREAD_SLEEP_ERR "thread library error: main thread cannot sleep"



static Thread* threads[MAX_THREAD_NUM];
static int current_tid =0;
static int total_quantums =0;
static int quantum_usecs_global =0;
static std::queue<int> ready_queue;
static bool is_timer_interrupt = false;
sigset_t set;

void scheduler_handler(int sig);

void uthread_cleanup()
{
    for (int i = 0; i < MAX_THREAD_NUM; ++i)
    {
        if (threads[i] != nullptr)
        {
            delete threads[i];
            threads[i] = nullptr;
        }
    }
    std::queue<int> empty;
    std::swap(ready_queue, empty);
}

void timer_interrupt_handler(int sig)
{
    is_timer_interrupt = true;
    scheduler_handler(sig);
    is_timer_interrupt = false;
}

void ignoreClock() {
    if (sigprocmask(SIG_BLOCK, &set, NULL) == FAIL) {
       std::cout<< "signal mask error" << std::endl;
    }
}

/**
 * Tells the system to accept the clock signals from the real and virtual timer.
 */
void acceptClockSignal() {
    if (sigprocmask(SIG_UNBLOCK, &set, NULL) == FAIL) {
        std::cout<< "signal mask error" << std::endl;
    }
}

void thread_start(){
    int tid =uthread_get_tid();
    if(threads[tid]== nullptr || !threads[tid]->active){
        std::cout <<" OUT" << std::endl;
        exit(1);
    }
    void (*entry)() = threads[tid]->get_entry_point();

    if(entry){
        entry();
    }
    uthread_terminate(tid);
    while(true){

    }
}


int uthread_init(int quantum_usecs){
    if (quantum_usecs <= 0){
        std::cerr << QUANTUM_USECS_ERR << std::endl;
        return FAIL;
    }

    quantum_usecs_global = quantum_usecs;
    total_quantums = 1;
    current_tid = 0;

    threads[0] = new Thread();
    threads[0]->active = 1;
    threads[0]->quantum_count = 1;

    while (!ready_queue.empty()){
        ready_queue.pop();
    }

    struct sigaction sa = {};
    sa.sa_handler = &scheduler_handler;
    if (sigaction(SIGVTALRM, &sa, nullptr) < 0){
        std::cerr << SIGACTION_ERR << std::endl;
        exit(1);
    }

    struct itimerval timer;
    timer.it_value.tv_sec = quantum_usecs_global / 1000000;
    timer.it_value.tv_usec = quantum_usecs_global % 1000000;
    timer.it_interval.tv_sec = timer.it_value.tv_sec;
    timer.it_interval.tv_usec = timer.it_value.tv_usec;

    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr) < 0){
        std::cerr << TIMER_ERR << std::endl;
        exit(1);
    }

    return SUCCESS;
}

void scheduler_handler(int sig)
{

    sigemptyset(&set);
    sigaddset(&set, SIGVTALRM);
    sigprocmask(SIG_BLOCK, &set, nullptr);

    if (sig == SIGVTALRM)
    {
        for (int i = 0; i < MAX_THREAD_NUM; ++i)
        {
            if (threads[i] && threads[i]->active &&
                threads[i]->getState() == BLOCKED &&
                threads[i]->sleep_quantums > 0)
            {
                threads[i]->sleep_quantums--;

                if (threads[i]->sleep_quantums == 0 && !threads[i]->manually_blocked)
                {
                    threads[i]->setState(READY);
                    ready_queue.push(i);
                }
            }
        }

    }

    // Save current thread state
    if (threads[current_tid] && threads[current_tid]->active)
    {
        if (sigsetjmp(threads[current_tid]->get_env()[0], 1) == 1)
        {
            sigprocmask(SIG_UNBLOCK, &set, nullptr);
            return;
        }

        if (threads[current_tid]->getState() == RUNNING)
        {
            threads[current_tid]->setState(READY);
            if (threads[current_tid]->getState() == READY)
            {
                ready_queue.push(current_tid);
            }
        }
    }

    int next_tid = -1;
    while (!ready_queue.empty())
    {
        int tid = ready_queue.front();
        ready_queue.pop();

        if (threads[tid] && threads[tid]->active && threads[tid]->env_initialized &&
            threads[tid]->getState() == READY)
        {
            next_tid = tid;
            break;
        }
    }

    if (next_tid == -1)
    {
        std::cerr << "FATAL: No valid threads to schedule." << std::endl;
        exit(1);
    }

    current_tid = next_tid;
    threads[current_tid]->setState(RUNNING);


    if (sig == SIGVTALRM)
    {
        total_quantums++;
        threads[current_tid]->quantum_count++; // optional
    }
    sigprocmask(SIG_UNBLOCK, &set, nullptr);
    siglongjmp(threads[current_tid]->get_env()[0], 1);
}




int uthread_spawn(thread_entry_point entry_point){
    ignoreClock();
    if (entry_point == nullptr){
        std::cerr << INVALID_ENTRY_PTR << std::endl;
        return FAIL;
    }

    int tid = -1;
    for (int i = 0; i < MAX_THREAD_NUM; ++i){
        if (threads[i] == nullptr || !threads[i]->active){
            tid = i;
            break;
        }
    }

    if (tid == -1){
        std::cerr << UNAVAILABLE_THREAD_ERR << std::endl;
        return FAIL;
    }
    threads[tid] = new Thread(entry_point, tid);
    threads[tid]->active = 1;
    threads[tid]->setState(READY);
    threads[tid]->set_quantum_count(0);


    sigjmp_buf* env = threads[tid]->get_env();
    if(sigsetjmp(*env,1)==0) {
        address_t sp = (address_t)(threads[tid]->getStack()) + STACK_SIZE - sizeof(address_t);
        sp -= sp%16;
        address_t pc = (address_t)(thread_start);


        (*env)->__jmpbuf[JB_SP] = translate_address(sp);
        (*env)->__jmpbuf[JB_PC] = translate_address(pc);
        sigemptyset(&(*env)->__saved_mask);
        threads[tid]->env_initialized = 1;
    }
    ready_queue.push(tid);
    acceptClockSignal();
    return tid;
}

int uthread_terminate(int tid){
    ignoreClock();
    if (tid < 0 || tid >= MAX_THREAD_NUM || threads[tid] == nullptr || !threads[tid]->active){
        std::cerr << TID_VALIDATION_ERR << std::endl;
        return FAIL;
    }

    // check if main thread
    if (tid == 0)
    {
        // clean all other threads
        for (int i = 1; i < MAX_THREAD_NUM; ++i){
            if (threads[i] != nullptr){
                delete threads[i];
                threads[i] = nullptr;
            }
        }
        delete threads[tid];
        threads[tid] = nullptr;

        while (!ready_queue.empty()){
            ready_queue.pop();
        }
        exit(0);
    }
    // remove thread from ready_queue
    ThreadState state = threads[tid]->getState();
    if (state == READY){
        std::queue<int> new_queue;
        while (!ready_queue.empty()){
            int curr_tid = ready_queue.front();
            ready_queue.pop();
            if (curr_tid != tid){
                new_queue.push(curr_tid);
            }
        }
        ready_queue = new_queue;
    }

    if (tid == current_tid) {
        threads[tid]->active = 0;
        delete threads[tid];
        threads[tid] = nullptr;
        scheduler_handler(SIGVTALRM);
    } else {
        delete threads[tid];
        threads[tid] = nullptr;
    }
    acceptClockSignal();
    return SUCCESS;
}



int uthread_block(int tid){
    // check tid validation and if thread is active
    ignoreClock();
    if (tid < 0 || tid >= MAX_THREAD_NUM || threads[tid] == nullptr || !threads[tid]->active){
        std::cerr << TID_VALIDATION_ERR << std::endl;
        return FAIL;
    }

    //cannot block the main thread
    if (tid == 0){
        std::cerr << MAIN_THREAD_BLOCK_ERR << std::endl;
        return FAIL;
    }

    //if blocking a blocked thread, do nothing
    if (threads[tid]->getState() == BLOCKED && threads[tid]->manually_blocked){
        return SUCCESS;
    }

    //remove the thread from ready_queue to block it
    if (threads[tid]->getState() == READY){
        std::queue<int> new_queue;
        while (!ready_queue.empty()){
            int curr_tid = ready_queue.front();
            ready_queue.pop();
            if (curr_tid != tid){
                new_queue.push(curr_tid);
            }
        }
        ready_queue = new_queue;
    }
    threads[tid]->manually_blocked = true;
    threads[tid]->setState(BLOCKED);

    //if blocking the current running thread, then switch
    if (tid == current_tid){
        scheduler_handler(0);
    }
    acceptClockSignal();
    return SUCCESS;
}

int uthread_resume(int tid){
    ignoreClock();
    // check tid validation and if thread is active
    if (tid < 0 || tid >= MAX_THREAD_NUM || threads[tid] == nullptr || !threads[tid]->active){
        std::cerr << TID_VALIDATION_ERR << std::endl;
        return FAIL;
    }

    if (tid == 0 || tid == current_tid || threads[tid]->getState()==READY){
        return SUCCESS;
    }

    threads[tid]->manually_blocked = false;
    if (threads[tid]->sleep_quantums == 0)
    {
        threads[tid]->setState(READY);
        ready_queue.push(tid);
    }
    acceptClockSignal();
    return SUCCESS;
}

int uthread_sleep(int num_quantums){
    ignoreClock();
    if (num_quantums <= 0)
    {
        std::cerr << NUM_OF_QUANTUMS_ERR << std::endl;
        return FAIL;
    }

    if (current_tid == 0)
    {
        std::cerr << MAIN_THREAD_SLEEP_ERR << std::endl;
        return FAIL;
    }

    threads[current_tid]->sleep_quantums = num_quantums;

    threads[current_tid]->setState(BLOCKED);

    scheduler_handler(0);
    acceptClockSignal();
    return SUCCESS;
}

int uthread_get_tid()
{
    return current_tid;
}

int uthread_get_total_quantums()
{
    return total_quantums;
}

int uthread_get_quantums(int tid){
    ignoreClock();
    if (tid < 0 || tid >= MAX_THREAD_NUM || threads[tid] == nullptr || !threads[tid]->active)
    {
        std::cerr << TID_VALIDATION_ERR << std::endl;
        return FAIL;
    }
    acceptClockSignal();
    return threads[tid]->quantum_count;
}
