#ifndef EXECUTOR_REPLICATE_THREAD_H_
#define EXECUTOR_REPLICATE_THREAD_H_

#include "config.h"
#include "db_thread.h"


class ReplicateThread : DBThread
{
private:
    
    ThreadID replicate_thread_id_;

public:
    ReplicateThread(ThreadID thread_id, ThreadID replicate_thread_id, ProcID process_id = 0);
    ~ReplicateThread();
    
    void Run();
    
};





#endif