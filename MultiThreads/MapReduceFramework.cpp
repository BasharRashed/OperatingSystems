
#include "MapReduceFramework.h"
#include "MapReduceClient.h"

#include "Barrier.h"

#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>

// ---------- Forward declarations ----------
struct JobContext;
struct ThreadContext;
void shuffle(JobContext* job);
void reduceWorker(ThreadContext* tc);

// ---------- ThreadContext ----------
struct ThreadContext {
    int thread_id;
    JobContext* job;
};

// ---------- JobContext ----------
struct JobContext {
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;

    std::vector<std::thread> threads;
    std::vector<ThreadContext> thread_contexts;

    std::atomic<int> input_index;
    std::atomic<int> map_progress;
    std::vector<IntermediateVec> intermediate_vectors;

    std::mutex output_mutex;
    std::mutex state_mutex;
    JobState state;

    std::vector<IntermediateVec> shuffled_queue;
    std::mutex queue_mutex;

    std::atomic<int> reduce_progress;
    int total_reduce_groups; // total number of grouped vectors

    Barrier* barrier;

    int total_input;
    int num_threads;
    bool joined;

    JobContext(const MapReduceClient& client,
               const InputVec& inputVec,
               OutputVec& outputVec,
               int numThreads)
            : client(client),
              inputVec(inputVec),
              outputVec(outputVec),
              input_index(0),
              map_progress(0),
              state({UNDEFINED_STAGE, 0}),
              reduce_progress(0),
              total_reduce_groups(0),
              barrier(new Barrier(numThreads)),
              total_input(static_cast<int>(inputVec.size())),
              num_threads(numThreads),
              joined(false) {}
};

// ---------- emit2 ----------
void emit2(K2* key, V2* value, void* context) {
    auto* tc = static_cast<ThreadContext*>(context);
    JobContext* job = tc->job;

    job->intermediate_vectors[tc->thread_id].emplace_back(key, value);
}
// ****************************** ONLY WORKS FOR /r**************************
bool isCarriageReturnKey(K3* key) {
    // Assume KChar has vtable then 'char c' immediately after.
    // The 'c' will be at offset sizeof(void*) (vtable ptr size)
    char c = *(char*)(((char*)key) + sizeof(void*));
    return c == 13;
}
void emit3(K3* key, V3* value, void* context) {
    auto* tc = static_cast<ThreadContext*>(context);
    JobContext* job = tc->job;
    std::lock_guard<std::mutex> lock(job->output_mutex);
    job->outputVec.emplace_back(key, value);
}


// ---------- Map Worker Thread Function ----------
void mapWorker(ThreadContext* tc) {
    JobContext* job = tc->job;

    {
        std::lock_guard<std::mutex> lock(job->state_mutex);
        job->state.stage = MAP_STAGE;
    }

    int index = job->input_index.fetch_add(1);
    while (index < job->total_input) {
        const InputPair& pair = job->inputVec[index];
        job->client.map(pair.first, pair.second, tc);
        job->map_progress++;
        index = job->input_index.fetch_add(1);
    }

    IntermediateVec& vec = job->intermediate_vectors[tc->thread_id];
    std::sort(vec.begin(), vec.end(), [](const IntermediatePair& a, const IntermediatePair& b) {
        return *(a.first) < *(b.first);
    });

    job->barrier->barrier();  // Sync before shuffle/reduce
    if (tc->thread_id == 0) {
        shuffle(job);
    }
    reduceWorker(tc);
}

// ---------- startMapReduceJob ----------
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel) {

    auto* job = new JobContext(client, inputVec, outputVec, multiThreadLevel);

    job->intermediate_vectors.resize(multiThreadLevel);
    job->thread_contexts.resize(multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; ++i) {
        job->thread_contexts[i] = { i, job };
    }

    try {
        for (int i = 0; i < multiThreadLevel; ++i) {
            job->threads.emplace_back(mapWorker, &job->thread_contexts[i]);
        }
    } catch (const std::system_error& e) {
        std::cout << "system error: failed to create thread" << std::endl;
        exit(1);
    }

    return static_cast<JobHandle>(job);

}

void shuffle(JobContext* job) {
    {
        std::lock_guard<std::mutex> lock(job->state_mutex);
        job->state.stage = SHUFFLE_STAGE;
    }

    auto& vectors = job->intermediate_vectors;

    while (true) {
        K2* maxKey = nullptr;

        // Step 1: Find the largest key among back elements
        for (const auto& vec : vectors) {
            if (!vec.empty()) {
                K2* candidate = vec.back().first;
                if (!maxKey || (*maxKey < *candidate)) {
                    maxKey = candidate;
                }
            }
        }

        if (!maxKey) {
            break;  // All vectors are empty
        }

        // Step 2: Collect all pairs with this maxKey
        IntermediateVec group;

        for (auto& vec : vectors) {
            while (!vec.empty() && !(*vec.back().first < *maxKey) && !(*maxKey < *vec.back().first)) {
                group.push_back(vec.back());
                vec.pop_back();
            }
        }

        job->shuffled_queue.push_back(std::move(group));
        job->total_reduce_groups++;
    }

}
void reduceWorker(ThreadContext* tc) {
    JobContext* job = tc->job;


    job->barrier->barrier();  // Ensure all threads completed shuffle

    {
        std::lock_guard<std::mutex> lock(job->state_mutex);
        job->state.stage = REDUCE_STAGE;
    }

    while (true) {
        IntermediateVec group;

        {
            std::lock_guard<std::mutex> lock(job->queue_mutex);
            if (job->shuffled_queue.empty()) {
                break;
            }

            group = std::move(job->shuffled_queue.back());
            job->shuffled_queue.pop_back();
        }

        job->client.reduce(&group, tc);  // calls emit3 internally
        job->reduce_progress++;
    }
}

void waitForJob(JobHandle handle) {
    auto* job = static_cast<JobContext*>(handle);

    // Only one thread may join
    static std::mutex join_mutex;
    std::lock_guard<std::mutex> lock(join_mutex);

    if (!job->joined) {
        for (std::thread& t : job->threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        job->joined = true;
    }
}


void getJobState(JobHandle handle, JobState* state) {
    *state = JobState{UNDEFINED_STAGE, 0.0f};
    auto* job = static_cast<JobContext*>(handle);

    stage_t current_stage;
    {
        std::lock_guard<std::mutex> lock(job->state_mutex);
        current_stage = job->state.stage;
    }

    float percentage = 0.0f;

    switch (current_stage) {
        case MAP_STAGE:
            percentage = 100.0f * job->map_progress.load() / job->total_input;
            break;

        case SHUFFLE_STAGE:
            percentage = 0.0f;
            break;

        case REDUCE_STAGE:
            if (job->total_reduce_groups > 0) {
                percentage = 100.0f * job->reduce_progress.load() / job->total_reduce_groups;
            } else {
                percentage = 100.0f;  // ✅ Initialize explicitly to avoid garbage
            }
            break;

        default:
            percentage = 0.0f;
            break;
    }

    state->stage = current_stage;
    state->percentage = percentage;
}
#include <cstdio>
void closeJobHandle(JobHandle handle) {
    auto* job = static_cast<JobContext*>(handle);



    // Join all threads, if not already joined
    static std::mutex join_mutex;
    std::lock_guard<std::mutex> lock(join_mutex);

    if (!job->joined) {
        for (std::thread& t : job->threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        job->joined = true;
    }

    delete job->barrier;
    delete job;
}
