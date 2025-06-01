#ifndef BENCHMARK_WORKLOAD_H_
#define BENCHMARK_WORKLOAD_H_

#include "config.h"

#include "db_spin_lock.h"

#include <queue>


class Transaction;
class TxnIdentifier;
class TxnContext;
class Client;

class Workload
{
protected:

    // /** 系统执行事务线程的数量 **/
    // uint64_t      txn_thread_num_;
    // // /** 为每个线程创建多少个事务 **/
    // // uint64_t      txn_num_per_thread_;

    // /** 为每个事务线程分配单独的待执行事务队列 **/
    // std::queue<Transaction*>*            txns_per_threads_;
    // alignas(CACHE_LINE_SIZE) DBSpinLock  waiting_queue_lock_[g_txn_thread_num];


    /*
     * 客户端
     * 每个客户端对应一个执行线程，一个执行线程对应多个客户端
     */
    Client* clients_[g_client_total_num];


public:

    Workload();
    ~Workload();

    /* 
     * 生成事务
     * 生成哪些事务、每种事务比例如何，由具体的负载实现该函数
     */
    // virtual Transaction* GenTxn(ThreadID thread_id) = 0;
    virtual Transaction* GenTxn() = 0;


    // virtual bool InsertNewTxn(ThreadID thread_id, Transaction* new_txn) = 0;
    // bool         WaitingQueueIsFull(ThreadID thread_id);

    //获取指定线程下一个待执行事务
    // virtual Transaction* GetNextTxn(uint64_t thread_id);
    

    // void    AppendNewTxn(ClientID client_id, Transaction* new_txn);
    
    Client* GetClient(ClientID client_id);



    /*
     * 用于标记执行事务的线程状态 
     */
    // //还在执行事务的线程数量
    // volatile uint32_t working_thread_num_;
    // //所有线程是否均执行结束
    // volatile bool     workload_finish_;

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
    Transaction();
    // Transaction(ThreadID thread_id, uint64_t local_txn_id);
    ~Transaction();

    /* 
     * 生成事务输入参数
     * 第一个指定了shard_id，表示事务输入要在该shard_id中
     * 第二个未指定shard_id，表示随机生成所属的shard
     */
    virtual void GenInputData(uint64_t  shard_id) = 0;
    virtual void GenInputData() = 0;

    //the logic of transaction
    virtual RC   RunTxn() = 0;


    //事务ID
    void           InitTxnIdentifier(ClientID client_id, uint64_t local_txn_id);
    TxnIdentifier* GetTxnIdentifier();


    //设置管理事务的上下文
    void SetTxnContext(TxnContext* txn_context);


    //事务类型
    uint64_t       txn_type_;

    /*
     * 事务抵达系统的时间
     * 在当前的设计中，系统生成只读事务后，如果事务线程正在执行其他只读事务，
     * 则会先放入事务线程的等待队列中。arrive_ct_即为事务生成的时间。
     * 
     * 在只读事务执行前，备节点通过向主节点获取当前系统全局提交时间戳作为只读一致快照，保证
     * 只读事务的强一致性。
     * arrive_ct_主要用于备节点的Linear Lamport timestamp机制（PolarDB-SCC vldb 2023），
     * 该机制在保证只读事务强一致性前提下，将备节点多个事务获取时间戳请求合并为一个，可大幅减少
     * 备节点获取全局时间戳的开销，并减少网络带宽压力。
     * Linear Lamport timestamp机制实现请见，global_ts_comm.h -> FetchGlobalCTS()方法
     */
    ClockTime      arrive_ct_;

    /* 
     * 读时间戳
     * 如果是主副本上最新提交的事务的提交时间戳，则为强一致读
     * 如果是备副本的可见时间戳，则为过时读
     */
    Timestamp      read_ts_;


    /* 
     * 判断事务是否为分布式事务
     * local_shard_id_表示事务所在shard
     * remote_shard_id_表示在分布式事务中，访问的另一个shard
     * 
     * 注意，目前事务最多访问两个不同的shard，不支持事务访问三个及以上shard
     */
    // bool     is_local_txn_;
    // ShardID  local_shard_id_;
    // ShardID  remote_shard_id_;

};



#endif
