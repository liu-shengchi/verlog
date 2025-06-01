#ifndef EXECUTOR_REPLICATE_THREAD_H_
#define EXECUTOR_REPLICATE_THREAD_H_

#include "config.h"
#include "db_thread.h"


class ReplicateThread : public DBThread
{
private:


public:
    ReplicateThread(ThreadID thread_id, ProcID process_id);
    ~ReplicateThread();
    
    void Run();
    
};


#endif