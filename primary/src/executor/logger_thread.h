#ifndef  LOG_THREAD_H_
#define  LOG_THREAD_H_

#include "config.h"

#include "db_thread.h"


class LogBuffer;


class LoggerThread : public DBThread
{
private:
    
    LogBufID   log_buf_id_;
    LogBuffer* log_buffer_;
    

public:
    LoggerThread(ThreadID thread_id, ProcID process_id = 0);
    ~LoggerThread();

    void Run();

};





#endif