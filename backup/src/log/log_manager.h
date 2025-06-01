#ifndef LOG_MANAGER_H_
#define LOG_MANAGER_H_

#include "config.h"

class LogBuffer;


/* 
 * 在当前设计下，事务在提交前在本地构造事务日志，并将日志写入日志缓冲区中，之后返回。
 * 日志的持久化不在事务的关键路径中，当日志写入日志缓冲区后，事务进入预提交状态，被放入预提交队列。
 * 当事务的日志被持久化后，预提交队列中的事务才允许提交。
 */
class LogManager
{
private:

    /* 
     * 全局日志缓冲区
     * 
     */
    uint64_t    log_buffer_num_;
    LogBuffer*  log_buffers_[g_log_buffer_num];

public:
    LogManager();
    ~LogManager();

    LogBuffer* GetLogBuffer(LogBufID log_buf_id);

    LogBufID   GetLogBufID(ThreadID thread_id, DBThreadType thread_type);

    MemPtr     log_buf_start_ptr_[g_log_buffer_num];
    
};


#endif