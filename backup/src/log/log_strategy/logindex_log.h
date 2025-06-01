#ifndef LOG_STRATEGY_LOGINDEX_LOG_H_
#define LOG_STRATEGY_LOGINDEX_LOG_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == LOGINDEX_LOG

class LogReplayContext;



/*************************
 * 日志索引策略
 *************************/

class LogIndexLog : public LogStrategy
{
private:
    
public:
    LogIndexLog();
    ~LogIndexLog();

    void DeconstructTxnLog(LogReplayContext* log_replay_context);

    void ReplayTxnLog(LogReplayContext* log_replay_context);

    void FinishReplayTxn(LogReplayContext* log_replay_context);

};


class LogIndexTxnMeta : public LogTxnMeta
{
public:
    LogIndexTxnMeta();
    ~LogIndexTxnMeta();
};




class LogIndexTupleMeta : public LogTupleMeta
{
public:
    LogIndexTupleMeta();
    ~LogIndexTupleMeta();

    LogBufID prev_log_buf_id_;
    LogLSN   prev_log_lsn_;
    MemPtr   prev_log_mem_ptr_;

    /* 
     * 元组日志的起始lsn和内存指针
     */
    LogLSN     logindex_lsn_;
    MemPtr     logindex_ptr_;

    void Reset();

    void CopyLogTupleMeta(LogIndexTupleMeta* log_tuple_meta);
};





#endif


#endif