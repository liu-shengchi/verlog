#ifndef LOG_STRATEGY_SERIAL_H_
#define LOG_STRATEGY_SERIAL_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == SERIAL_LOG

/*******************************************
串行日志
*******************************************/
class SerialLog : public LogStrategy
{
private:

public:
    SerialLog();
    ~SerialLog();

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


class SerialTupleMeta : public LogTupleMeta
{
public:
    SerialTupleMeta();
    ~SerialTupleMeta();

    void Reset();

    void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta);

};


class SerialTxnMeta : public LogTxnMeta
{
public:
    SerialTxnMeta();
    ~SerialTxnMeta();

    void Reset();

};

#endif


#endif