#ifndef BENCHMARK_WORKLOAD_H_
#define BENCHMARK_WORKLOAD_H_

#include "config.h"

#include <queue>


class Transaction;
class TxnIdentifier;
class TxnContext;

// class TxnQueue;


class Workload
{
protected:

    /** 系统执行事务线程的数量 **/
    uint64_t      txn_thread_num_;
    /** 为每个线程创建多少个事务 **/
    uint64_t      txn_num_per_thread_;
    /** 为每个事务线程分配单独的待执行事务队列 **/
    std::queue<Transaction*>* txns_per_threads_;


public:

    Workload();
    ~Workload();

    

    /* 
     * 生成事务
     * 生成哪些事务、每种事务比例如何，由具体的负载实现该函数
     */
    virtual Transaction* GenTxn() = 0;

    virtual void InsertNewTxn(ThreadID thread_id, Transaction* new_txn) = 0;

    //获取指定线程下一个待执行事务
    virtual Transaction* GetNextTxn(uint64_t thread_id);
    

    volatile uint32_t warmup_thread_num_;
    volatile bool     warmup_finish_;

    volatile bool     testing_finish_;

    /*
     * 用于标记执行事务的线程状态 
     */
    //还在执行事务的线程数量
    volatile uint32_t working_thread_num_;
    //所有线程是否均执行结束
    volatile bool     workload_finish_;

};


class Transaction
{
protected:
    //transaction identification
    //thread_id + local_txn_id
    TxnIdentifier* txn_identifier_;
    
    /*** 事务执行的上下文 ***/
    TxnContext*    txn_context_;

public:
    Transaction(){}
    Transaction(ThreadID thread_id, uint64_t local_txn_id);
    ~Transaction();

    /* 
     * 生成事务输入参数
     * 第一个指定了shard_id，表示事务输入要在该shard_id中
     * 第二个未指定shard_id，表示随机生成所属的shard
     */
    virtual void GenInputData(ShardID  shard_id) = 0;
    virtual void GenInputData() = 0;

    //the logic of transaction
    virtual RC RunTxn() = 0;


    void InitTxnIdentifier(ThreadID thread_id, uint64_t local_txn_id);
    TxnIdentifier* GetTxnIdentifier();

    void SetTxnContext(TxnContext* txn_context);

    //事务类型
    uint64_t       txn_type_;

    /* 
     * 判断事务是否为分布式事务
     * local_shard_id_表示事务所在shard
     * remote_shard_id_表示在分布式事务中，访问的另一个shard
     * 
     * 注意，目前事务最多访问两个不同的shard，不支持事务访问三个及以上shard
     */
    bool     is_local_txn_;
    ShardID  local_shard_id_;
    ShardID  remote_shard_id_;


    /* 
     * 记录事务日志最末尾所在的LSN
     * 目前用于事务预提交策略，避免事务并发控制临界区包含日志持久化过程
     */
    LogBufID log_buffer_id_;
    LogLSN   last_log_lsn_;

#if  DISTRIBUTED_LOG
    LogBufID remote_log_buffer_id_;
    LogLSN   remote_last_log_lsn_;
#endif


};






#endif
