#ifndef LOG_STRATEGY_TAURUS_H_
#define LOG_STRATEGY_TAURUS_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == TAURUS_LOG

class TxnContext;


/*******************************************
Taurus并行日志
*******************************************/

class TaurusLog : public LogStrategy
{
private:
    
public:
    TaurusLog();
    ~TaurusLog();

    
    void     AccessTuple(TxnContext* txn_context, AccessEntry* access_entry);

    void     CommitTuple(TxnContext* txn_context, AccessEntry* access_entry);

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



class TaurusTupleMeta : public LogTupleMeta
{
public:
    TaurusTupleMeta();
    ~TaurusTupleMeta();

    //元组write/read的lsn向量
    LogLSN write_lsn_vector_[g_log_buffer_num];
    LogLSN read_lsn_vector_[g_log_buffer_num];

    void Reset();

    void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta);

    void ReadLVWiseMax(LogLSN lsn_verctor[]);
    void WriteLVWiseMax(LogLSN lsn_verctor[]);

};


class TaurusTxnMeta : public LogTxnMeta
{
public:
    TaurusTxnMeta();
    ~TaurusTxnMeta();

    //事务的lsn向量
    LogLSN txn_lsn_vector_[g_log_buffer_num];

    void LVWiseMax(LogLSN lsn_verctor[]);
    void Reset();

};


#endif

#endif