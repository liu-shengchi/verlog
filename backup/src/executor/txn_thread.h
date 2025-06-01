#ifndef EXECUTOR_TXN_THREAD_H_
#define EXECUTOR_TXN_THREAD_H_


#include "config.h"

#include "db_thread.h"

#include <pthread.h>
#include <queue>



class Transaction;
class TxnContext;
class LogBuffer;
class Client;


class TxnThread : public DBThread
{
private:
    
    /* 
     * 每当事务线程执行一个事务，local_txn_id_ +1，与thread_id_构成 <thread_id_, local_txn_id_>
     * 唯一标识系统中的事务。
     */
    ThreadID txn_thread_id_;
    
    TxnID    local_txn_id_;


    /* 
     * 与事务执行线程关联的client
     */
    Client* clients_[g_client_num_per_txn_thread];


    /*********************************************/
    /************* 事务管理相关数据结构 ************/
    /*********************************************/
    // //当前事务线程执行的事务，包括事务的逻辑、输入参数等信息
    // Transaction* current_txn_;

    // //事务的上下文
    // TxnContext*  txn_context_;


    /**************** 友元类和函数 *****************/
    friend class TxnContext;
    


    /*****  *****/
    // void ExecuteWorkload(SystemState system_state);
    
    void ResetTxnThread();


public:
    TxnThread(ThreadID thread_id, ThreadID txn_thread_id, ProcID process_id = 0);
    ~TxnThread();

    void Run();
    
    void AssociateClient();

    ThreadID GetTxnThreadID();
};


#endif
