#ifndef EXECUTOR_TXN_THREAD_H_
#define EXECUTOR_TXN_THREAD_H_


#include "config.h"

#include "db_thread.h"

#include <pthread.h>
#include <queue>



class Transaction;
class TxnContext;
class LogBuffer;

class TxnThread : public DBThread
{
private:
    
    /* 
     * 每当事务线程执行一个事务，local_txn_id_ +1，与thread_id_构成 <thread_id_, local_txn_id_>
     * 唯一标识系统中的事务。
     */
    TxnID    local_txn_id_;

    /*********************************************/
    /************* 事务管理相关数据结构 ************/
    /*********************************************/
    //当前事务线程执行的事务，包括事务的逻辑、输入参数等信息
    Transaction* current_txn_;

    //事务的上下文
    TxnContext*  txn_context_;

    /* 
     * 事务预提交列表
     * 根据事务ACID属性，需要保证日志持久化，甚至复制到备节点后，
     * 才允许事务提交。这就导致事务在执行阶段结束后，会阻塞较长一段时间，
     * 等待完成持久化与高可用，降低事务线程的效率。
     * 因此，我们实现预提交机制，把已经将日志写入到日志缓冲区内的事务放入预提交列表，
     * 事务线程继续执行新事务。每隔一段时间，判断预提交事务的日志完成持久化与高可用，
     * 如果完成，则允许事务提交；如果未完成，则继续放在预提交列表中。
     */
    std::queue<Transaction*> pre_commit_queue_;

    // std::queue<Transaction*> abort_queue_;

    /*********************************************/
    /************** 日志管理相关数据结构 ***********/
    /*********************************************/
    /* 
     * 当前事务线程绑定的日志缓冲区
     * 在当前的设计中，每个事务线程均和一个日志缓冲区绑定，除非存在分布式日志策略（比如，Plover等日志分区），
     * 否则事务线程始终将事务日志写入到绑定的日志缓冲区。
     */
    LogBuffer*   local_log_buffer_;
    

    /**************** 友元类和函数 *****************/
    friend class TxnContext;
    


    void ExecuteWorkload(uint64_t executed_txn_num, SystemState sys_state);
    
    void CommitTxn(bool wait);


public:
    TxnThread(ThreadID thread_id, ProcID process_id = 0);
    ~TxnThread();

    void Run();
    
};


#endif
