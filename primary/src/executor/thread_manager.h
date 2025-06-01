#ifndef EXECUTOR_THREAD_MANAGER_H_
#define EXECUTOR_THREAD_MANAGER_H_

#include "config.h"


#include <pthread.h>


class TxnThread;
class LoadThread;
class LoggerThread;
class ReplicateThread;


class ThreadManager
{
private:

    /* 
     * load线程
     */
    uint32_t     load_thread_num_;
    LoadThread** load_threads_;
    pthread_t*   p_load_threads_;

    /* 
     * txn线程
     */
    uint32_t     txn_thread_num_;
    TxnThread**  txn_threads_;
    pthread_t*   p_txn_threads_;

    /* 
     * logger线程
     */
    uint32_t       logger_thread_num_;
    LoggerThread** logger_threads_;
    pthread_t*     p_logger_threads_;

    /*
     * replicate线程
     */
    uint32_t          replicate_thread_num_;
    ReplicateThread** replicate_threads_;
    pthread_t*        p_replicate_threads_;

    

public:

    pthread_barrier_t* execute_workload_barrier_;

    
    ThreadManager();
    ~ThreadManager();

    void CreateLoadThread();
    void JoinLoadThread();

    void CreateTxnThread();
    void JoinTxnThread();
    
    void CreateLoggerThread();
    void JoinLoggerThread();

    void CreateReplicateThread();
    void JoinReplicateThread();

};


#endif
