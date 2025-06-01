#include "thread_manager.h"

#include "global.h"

#include "load_thread.h"
#include "txn_thread.h"
#include "logger_thread.h"
#include "replicate_thread.h"

#include "tpcc_config.h"
#include "ycsb_config.h"
#include "smallbank_config.h"

#include <malloc.h>


ThreadManager::ThreadManager()
{
    load_thread_num_ = g_load_thread_num;
    load_threads_    = new LoadThread* [load_thread_num_];
    p_load_threads_  = new pthread_t[load_thread_num_];

    txn_thread_num_ = g_txn_thread_num;
    txn_threads_    = new TxnThread* [txn_thread_num_];
    p_txn_threads_  = new pthread_t[txn_thread_num_];

    logger_thread_num_ = g_logger_thread_num < g_log_buffer_num ? g_log_buffer_num : g_logger_thread_num;
    logger_threads_    = new LoggerThread* [logger_thread_num_];
    p_logger_threads_  = new pthread_t[logger_thread_num_];

    replicate_thread_num_ = g_replica_thread_num;
    replicate_threads_    = new ReplicateThread* [replicate_thread_num_];
    p_replicate_threads_  = new pthread_t[replicate_thread_num_];

    execute_workload_barrier_ = new pthread_barrier_t;
    //+1表示控制线程（main线程）也要参与事务开始执行的barrier
    pthread_barrier_init(execute_workload_barrier_, NULL, txn_thread_num_ + 1);

}

ThreadManager::~ThreadManager()
{
    for (uint64_t i = 0; i < load_thread_num_; i++)
    {
        delete load_threads_[i];
    }
    delete[] load_threads_;
    delete[] p_load_threads_;

    for (uint64_t i = 0; i < txn_thread_num_; i++)
    {
        delete txn_threads_[i];
    }
    delete[] txn_threads_;
    delete[] p_txn_threads_;

    for (uint64_t i = 0; i < logger_thread_num_; i++)
    {
        delete logger_threads_[i];
    }
    delete[] logger_threads_;
    delete[] p_logger_threads_;

    for (uint64_t i = 0; i < replicate_thread_num_; i++)
    {
        delete replicate_threads_[i];
    }
    delete[] replicate_threads_;
    delete[] p_replicate_threads_;
    
    delete execute_workload_barrier_;
}


void ThreadManager::CreateLoadThread()
{
#if  WORKLOAD_TYPE == TPCC_W

    uint64_t min_w_id = 1;
    uint64_t max_w_id = 0;
    uint64_t interval = g_warehouse_num / load_thread_num_;
    uint64_t mod = g_warehouse_num % load_thread_num_;
    
    for (uint64_t thread_id = 0; thread_id < load_thread_num_; thread_id++)
    {
        min_w_id = max_w_id + 1;
        max_w_id += interval;
        if (mod > thread_id)
        {
            max_w_id++;
        }
        
        load_threads_[thread_id] = new LoadThread(thread_id, thread_id, min_w_id, max_w_id);

        pthread_create( &p_load_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((LoadThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)load_threads_[thread_id] );

        printf("create load thread, thread id: %d \n", thread_id);
    }

#elif WORKLOAD_TYPE == YCSB_W

    uint64_t min_shard = 1;
    uint64_t max_shard = 0;
    uint64_t interval = g_ycsb_shard_num / load_thread_num_;
    uint64_t mod = g_ycsb_shard_num % load_thread_num_;

    for (uint64_t thread_id = 0; thread_id < load_thread_num_; thread_id++)
    {
        min_shard = max_shard + 1;
        max_shard += interval;
        if (mod > thread_id)
        {
            max_shard++;
        }
        
        load_threads_[thread_id] = new LoadThread(thread_id, thread_id, min_shard, max_shard);

        pthread_create( &p_load_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((LoadThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)load_threads_[thread_id] );

        printf("create load thread, thread id: %d \n", thread_id);
    }


#elif WORKLOAD_TYPE == SMALLBANK_W

    int64_t min_shard = 0;
    int64_t max_shard = -1;
    uint64_t interval = g_smallbank_shard_num / load_thread_num_;
    uint64_t mod = g_smallbank_shard_num % load_thread_num_;

    for (uint64_t thread_id = 0; thread_id < load_thread_num_; thread_id++)
    {
        min_shard = max_shard + 1;
        max_shard += interval;
        if (mod > thread_id)
        {
            max_shard++;
        }

        load_threads_[thread_id] = new LoadThread(thread_id, thread_id, min_shard, max_shard);

        pthread_create( &p_load_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((LoadThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)load_threads_[thread_id] );

        printf("create load thread, thread id: %d \n", thread_id);
    }

    // printf("undefined create load thead under smallbank workload!\n");
    // exit(0);

#endif


}

void ThreadManager::JoinLoadThread()
{
    printf("wait load threads finishing! \n");

    for (uint64_t thread_id = 0; thread_id < load_thread_num_; thread_id++)
    {
        pthread_join(p_load_threads_[thread_id], NULL);
    }

    printf("load threads finishing! \n");

}



void ThreadManager::CreateTxnThread()
{
    for (uint64_t thread_id = 0; thread_id < txn_thread_num_; thread_id++)
    {
        txn_threads_[thread_id] = new TxnThread(thread_id, thread_id);   //+1不使用第一个物理核

        
        printf("create txn thread, thread id: %d \n", thread_id);

        pthread_create( &p_txn_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((TxnThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)txn_threads_[thread_id] );
    }
}


void ThreadManager::JoinTxnThread()
{
    printf("wait txn threads finishing! \n");

    for (uint64_t i = 0; i < txn_thread_num_; i++)
    {
        pthread_join(p_txn_threads_[i], NULL);
    }

    printf("txn threads finishing! \n");
}


void ThreadManager::CreateLoggerThread()
{

    for (uint64_t thread_id = 0; thread_id < logger_thread_num_; thread_id++)
    {
        logger_threads_[thread_id] = new LoggerThread(thread_id + logger_thread_id_offset, thread_id + logger_thread_id_offset);

        printf("create logger thread, thread id: %d \n", thread_id);

        pthread_create( &p_logger_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((LoggerThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)logger_threads_[thread_id] );
    }
}


void ThreadManager::JoinLoggerThread()
{
    printf("wait logger threads finishing! \n");

    for (uint64_t i = 0; i < logger_thread_num_; i++)
    {
        pthread_join(p_logger_threads_[i], NULL);
    }

    printf("logger threads finishing! \n");
    
}


void ThreadManager::CreateReplicateThread()
{
    for (uint64_t thread_id = 0; thread_id < replicate_thread_num_; thread_id++)
    {
        replicate_threads_[thread_id] = new ReplicateThread(thread_id + replica_thread_id_offset, thread_id + replica_thread_id_offset);
        
        printf("create replicate thread, thread id: %d \n", thread_id + replica_thread_id_offset);

        pthread_create( &p_replicate_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((ReplicateThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)replicate_threads_[thread_id] );
    }
}


void ThreadManager::JoinReplicateThread()
{
    printf("wait replicate threads finishing! \n");

    for (uint64_t i = 0; i < replicate_thread_num_; i++)
    {
        pthread_join(p_replicate_threads_[i], NULL);
    }

    printf("replicate threads finishing! \n");
}
