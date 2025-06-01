#ifndef LOG_STRATEGY_H_
#define LOG_STRATEGY_H_

#include "config.h"

class LogReplayContext;


class LogStrategy
{
public:
    LogStrategy();
    ~LogStrategy();

    /* 
     * 对事务日志进行解析
     * 传入日志二进制数据，不同的日志策略采用不同的方式对日志内容进行解析。
     * 生成该事务日志的LogTxnMeta和事务日志种包含的每个元组的LogTupleMeta元信息。
     * 每个日志策略均需实现自己的解析策略。
     * 
     * 传入参数：
     * @param log_replay_context：日志回放上下文。该数据结构内包含需要解析的事务日志信息，
     *                            以及解析后，保存解析结果的数据结构。
     */
    virtual void DeconstructTxnLog(LogReplayContext* log_replay_context) = 0;

    /* 
     * 回放事务日志
     * 日志策略根据日志元数据，控制事务日志中每个元组的回放。
     */
    virtual void ReplayTxnLog(LogReplayContext* log_replay_context) = 0;

    /* 
     * 结束回放事务日志。
     * 修改相关元数据，推进备节点可见快照；
     * 释放日志回放过程中申请的临时空间；clear日志回放上下文
     */
    virtual void FinishReplayTxn(LogReplayContext* log_replay_context) = 0;

};


/* 
 * LogTxnMeta
 * 记录一个事务的日志元信息，用于提供解析日志所需的基本信息。
 */
class LogTxnMeta
{
public:
    LogTxnMeta();
    ~LogTxnMeta();

    /* 
     * 事务日志所在日志缓冲区和日志起始LSN
     */
    LogBufID log_buf_id_;
    LogLSN   start_lsn_;

    /* 
     * 日志的基本信息
     */
    uint64_t log_size_;     //日志大小
    // bool     log_buf_loop_; //日志是否循环
    // uint64_t leave_size_;   //如果循环，则剩余部分的大小
    
    /* 注意：关于事务日志跨log buffer的情况，主要问题是在后续解析、回放过程，需要大量的判断，
     *      确定某一日志区间是否跨越log buffer。
     * 优化思路：如果事务日志跨日志缓冲区，则申请一块新的内存空间，将两段日志拼接成空间连续的临时日志，
     *          后续操作针对该临时日志进行。当然，这块“临时日志”只能读取，不能对其写入更新，如果需要
     *          写入更新，不仅要对“临时日志”进行修改，还要对日志缓冲区中的“原始日志”进行修改。
     *          支持上述过程高效处理，需要合理设计数据结构。
     */
    // char*    temp_txn_log_; //如果循环，则申请临时空间，将事务日志保存在连续的临时空间中
    

    //日志起始位置的内存地址，转为char*可以用来操作内存空间
    MemPtr   log_start_ptr_;
    //解析日志的进度 [start_lsn_, start_lsn_ + log_deconstruct_offset_) 表示已经解析的日志
    // uint64_t log_deconstruct_offset_;

    /*
     * 事务的提交时间戳
     * 表示事务在备节点上的可见顺序，一致快照
     */
    Timestamp commit_ts_;


    virtual void Reset();

};


/* 
 * LogTupleMeta
 * 每个事务日志除了事务元信息外，还记录了更新的每个元组的修改信息。
 */
class LogTupleMeta
{
public:
    LogTupleMeta();
    ~LogTupleMeta();

    /*
     * 该条日志对应元组的访问信息。
     */
    //主键
    PrimaryKey primary_key_;
    //所在分区ID
    ShardID    shard_id_;
    //所属表ID
    TableID    table_id_;
    //操作类型：更新1、插入2、删除3
    AccessType opt_type_;
    //元组的大小，也是日志tuple_data的大小
    uint64_t   tuple_log_size_;
    

    typedef struct IndexAndOpt
    {
        IndexID    index_id_;
        AccessType index_opt_;
    }IndexAndOpt;
    
    uint64_t    index_and_opt_cnt_;
    IndexAndOpt index_and_opts_[MAX_INDEX_MODIFY_PER_LOG];
    
    /* 
     * 日志元组数据的起始lsn和内存指针
     */
    LogLSN     tuple_log_lsn_;
    MemPtr     tuple_log_ptr_;
    
    /* 
     * 该条元组日志所在事务的提交时间戳
     */
    Timestamp  commit_ts_;

    /* 
     * 该条元组日志所在的日志缓冲区和lsn区间[start_lsn_, end_lsn_)
     * 标识该条元组在日志中的位置和范围
     */
    LogLogicalPtr log_logical_ptr_;
    LogBufID      log_buf_id_;
    LogLSN        start_lsn_;
    LogLSN        end_lsn_;

    virtual void Reset();

    virtual void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta);

};




#endif
