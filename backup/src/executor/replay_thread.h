#ifndef EXECUTOR_REPLAY_THREAD_H_
#define EXECUTOR_REPLAY_THREAD_H_

#include "config.h"
#include "db_thread.h"

class LogBuffer;

class LogReplayContext;


class ReplayThread : public DBThread
{
private:

    ThreadID  replay_thread_id_;

    uint64_t  replay_log_buf_num_;
    uint64_t  replay_thread_index_;
    LogBuffer*        replay_log_buf_[g_log_buffer_num];
    LogReplayContext* log_replay_context_[g_log_buffer_num];

    void AllocLogBufToReplayer();

    friend class LogReplayContext;


public:
    ReplayThread(ThreadID thread_id, ThreadID replay_thread_id, ProcID processor_id = 0);
    ~ReplayThread();
    
    void Run();

};



#endif