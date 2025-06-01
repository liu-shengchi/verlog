#ifndef EXECUTOR_THREAD_MANAGER_H_
#define EXECUTOR_THREAD_MANAGER_H_

#include "config.h"


#include <pthread.h>

class DBThread;
class TxnThread;
class ReplayThread;
class ReclaimThread;
class ApplyWriteThread;
class LoggerThread;
class ReplicateThread;
class WorkloadThread;

class ThreadManager
{
private:

    /* 
     * txn线程
     */
    uint32_t     txn_thread_num_;
    TxnThread**  txn_threads_;
    pthread_t*   p_txn_threads_;

    /* 
     * replay线程
     */
    uint32_t       replay_thread_num_;
    ReplayThread** replay_threads_;
    pthread_t*     p_replay_threads_;

// #if    AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    /* 
     * reclaim线程
     */
    uint32_t        reclaim_thread_num_;
    ReclaimThread** reclaim_threads_;
    pthread_t*      p_reclaim_threads_;
// #endif



#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    /* 
     * apply writes线程
     */
    uint32_t           apply_thread_num_;
    ApplyWriteThread** apply_threads_;
    pthread_t*         p_apply_threads_;
#endif



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

    /*
     * workload线程
     */
    uint32_t          workload_thread_num_;
    WorkloadThread**  workload_threads_;
    pthread_t*        p_workload_threads_;


public:
    
    ThreadManager();
    ~ThreadManager();

    void CreateTxnThread();
    void JoinTxnThread();

    void CreateReplayThread();
    void JoinReplayThread();

    void CreateReclaimThread();
    void JoinReclaimThread();

#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    void CreateApplyThread();
    void JoinApplyThread();
#endif

    void CreateLoggerThread();
    void JoinLoggerThread();

    void CreateReplicateThread();
    void JoinReplicateThread();

    void CreateWorkloadThread();
    void JoinWorkloadThread();


    DBThread* GetDBThread(DBThreadType thread_type, ThreadID thread_id);

};


#endif
