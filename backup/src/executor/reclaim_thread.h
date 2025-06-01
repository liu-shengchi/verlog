#ifndef  EXECUTOR_RECLAIM_THREAD_H_
#define  EXECUTOR_RECLAIM_THREAD_H_

#include "config.h"

#include "db_thread.h"


class LogBuffer;

class Snapshot;

class ReclaimThread : public DBThread
{
private: 
    
    ThreadID  reclaim_thread_id_;
    
    Snapshot* snapshot_;

    uint64_t   reclaim_log_buf_num_;
    LogBuffer* reclaim_log_buf_[g_log_buffer_num];
    uint64_t   reclaim_thread_index_;

    void AllocLogBufToReclaimer();
    


public:
    ReclaimThread(ThreadID thread_id, ThreadID reclaim_thread_id, ProcID process_id = 0);
    ~ReclaimThread();

    void Run();

};






#endif
