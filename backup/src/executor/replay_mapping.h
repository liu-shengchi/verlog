#ifndef REPLAY_MAPPING_THREAD_H_
#define REPLAY_MAPPING_THREAD_H_

#include "config.h"


class LogBuffer;
class LogReplayContext;


class ReplayMapping
{
private:


    typedef struct ThreadToBuf
    {
        uint64_t  replay_log_buf_num_;
        uint64_t  replay_thread_index_;
    
        LogBuffer*        replay_log_buf_[g_log_buffer_num];
        LogReplayContext* log_replay_context_[g_log_buffer_num];    
    }ThreadToBuf;

    ThreadToBuf mapping[g_replay_thread_num];

public:
    ReplayMapping();
    ~ReplayMapping();



};



#endif