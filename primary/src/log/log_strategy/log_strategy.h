#ifndef LOG_STRATEGY_H_
#define LOG_STRATEGY_H_

#include "config.h"

class LogBuffer;

class IndexAndOpt;
class AccessEntry;
class Tuple;

class TxnContext;
class TxnLog;



class LogTupleMeta
{
public:
    LogTupleMeta();
    ~LogTupleMeta();

    virtual void Reset() = 0;

    /** 传入的日志元数据为source，将元数据复制到当前LogTupleMeta中 **/
    virtual void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta) = 0;
};


class LogTxnMeta
{
public:
    LogTxnMeta();
    ~LogTxnMeta();

    virtual void Reset() = 0;
};


class LogStrategy
{
protected:


public:
    LogStrategy();
    ~LogStrategy();

    // /* 
    //  * 计算事务在创建日志前所需的元数据log_txn_meta_，例如：
    //  * 1. TaurusLog
    //  *     计算事务的LSN向量
    //  * 2. GSNLog
    //  *     计算事务的GSN
    //  */
    // virtual void CalculateLogTxnMeta(TxnContext* txn_context) = 0;

    /*
     * 事务访问元组时，部分日志机制需要访问元组的日志元数据，以更新事务的元数据
     * 
     * 注意，LogStrategy->AccessTuple的调用应在CCStrategy->AccessTuple之后。
     * 由CCStrategy->AccessTuple对访问元组处理后，AccessEntry中的operate_tuple可供
     * 日志模块访问。
     */
    virtual void AccessTuple(TxnContext* txn_context, AccessEntry* access_entry) = 0;

    /*
     * 当事务进入提交阶段时，需要对元组的日志元数据进行修改。
     */
    virtual void CommitTuple(TxnContext* txn_context, AccessEntry* access_entry) = 0;

    /* 
     * 
     */
    virtual void CommitTuple(LogTupleMeta* log_tuple_meta, LogBufID log_buf_id, LogLSN log_lsn) = 0;

    /* 
     * 在事务线程本地日志缓冲区，构造事务日志。
     * 每种日志策略采用不同的日志格式、需要维护的日志元数据也不相同，
     * 每种日志策略根据需要实现自己的构造日志函数。
     * 
     * 传入参数：
     * 1. txn_context
     *     事务上下文，里面包含事务执行过程中访问元组的信息，以及事务本地日志空间。
     * 日志构造函数，通过访问相关信息，根据日志构造策略，在本地日志空间构造相应日志。
     * 
     * 2. commit_ts
     *     事务的提交时间戳，在可串行化隔离级别下，等价于事务串行顺序。用于标识该事务日志，
     * 可见顺序。主要用于备节点强一致读，和日志恢复。
     * 
     * @return: 日志大小
     */
    virtual uint64_t ConstructTxnLog(TxnContext* txn_context, Timestamp commit_ts = 0) = 0;

    /* 
     * 在数据库load阶段，初始化元组需要以事务日志形式写入日志缓冲区中，持久化以及同步到备节点。
     * 在当前的设计中，以元组为单位，一个元组一个事务日志。每个日志的commit_ts均为0。Load日志之间
     * 不存在依赖关系。
     */
    virtual uint64_t ConstructLoadLog(
                        char* log_entry, 
                        LogBufID log_buf_id, 
                        Tuple* tuple, 
                        AccessType access_type, 
                        ShardID shard_id, 
                        TableID table_id, 
                        uint64_t index_and_opt_cnt, 
                        IndexAndOpt index_and_opts[],
                        Timestamp commit_ts = 0,
                        LogTupleMeta* log_tuple_meta = nullptr) = 0;

};



#endif
