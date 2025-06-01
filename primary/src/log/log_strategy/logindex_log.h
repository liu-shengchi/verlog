#ifndef LOG_STRATEGY_LOGINDEX_LOG_H_
#define LOG_STRATEGY_LOGINDEX_LOG_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == LOGINDEX_LOG

/*************************
 * 日志索引
 *************************/

class LogIndexLog : public LogStrategy
{
private:
    
public:
    LogIndexLog();
    ~LogIndexLog();

    void     AccessTuple(TxnContext* txn_context, AccessEntry* access_entry);

    void     CommitTuple(TxnContext* txn_context, AccessEntry* access_entry = nullptr);

    void     CommitTuple(LogTupleMeta* log_tuple_meta, LogBufID log_buf_id, LogLSN log_lsn);

    uint64_t ConstructTxnLog(TxnContext* txn_context, Timestamp commit_ts = 0);
    
    uint64_t ConstructLoadLog(
                        char* log_entry, 
                        LogBufID log_buf_id, 
                        Tuple* tuple, 
                        AccessType access_type, 
                        ShardID shard_id, 
                        TableID table_id, 
                        uint64_t index_and_opt_cnt, 
                        IndexAndOpt index_and_opts[],
                        Timestamp commit_ts = 0,
                        LogTupleMeta* log_tuple_meta = nullptr);
    
};


class LogIndexTupleMeta : public LogTupleMeta
{
public:
    LogIndexTupleMeta();
    ~LogIndexTupleMeta();

    LogBufID log_buf_id_;
    LogLSN   log_lsn_;

    void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta);

    void Reset();

};


class LogIndexTxnMeta : public LogTxnMeta
{
private:
    
public:
    LogIndexTxnMeta();
    ~LogIndexTxnMeta();

    //事务写日志所在日志缓冲区ID
    LogBufID log_buf_id_;
    //事务写入操作的数量
    uint64_t write_access_cnt_;
    //每个写操作对应事务AccessEntry的下标
    uint16_t write_access_index_[MAX_WRITE_ACCESS_PER_TXN];
    //每个写操作，对应元组更新日志的LSN
    LogLSN   write_access_lsn_offset_[MAX_WRITE_ACCESS_PER_TXN];

    void Reset();
};


#endif


#endif