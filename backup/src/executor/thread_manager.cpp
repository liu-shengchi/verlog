#include "thread_manager.h"

#include "global.h"

#include "txn_thread.h"
#include "replay_thread.h"
#include "reclaim_thread.h"
#include "apply_write_thread.h"
#include "logger_thread.h"
#include "replicate_thread.h"
#include "workload_thread.h"

#include <malloc.h>


ThreadManager::ThreadManager()
{
    txn_thread_num_ = g_txn_thread_num;
    txn_threads_    = new TxnThread* [txn_thread_num_];
    p_txn_threads_  = new pthread_t[txn_thread_num_];

    replay_thread_num_ = g_replay_thread_num;
    replay_threads_    = new ReplayThread*[replay_thread_num_];
    p_replay_threads_  = new pthread_t[replay_thread_num_];

    reclaim_thread_num_ = g_reclaim_thread_num;
    reclaim_threads_    = new ReclaimThread*[reclaim_thread_num_];
    p_reclaim_threads_  = new pthread_t[reclaim_thread_num_];

#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    apply_thread_num_ = g_apply_thread_num;
    apply_threads_    = new ApplyWriteThread*[apply_thread_num_];
    p_apply_threads_  = new pthread_t[apply_thread_num_];
#endif

    logger_thread_num_ = g_logger_thread_num < g_log_buffer_num ? g_log_buffer_num : g_logger_thread_num;
    logger_threads_    = new LoggerThread* [logger_thread_num_];
    p_logger_threads_  = new pthread_t[logger_thread_num_];

    replicate_thread_num_ = g_replica_thread_num;
    replicate_threads_    = new ReplicateThread* [replicate_thread_num_];
    p_replicate_threads_  = new pthread_t[replicate_thread_num_];

    workload_thread_num_ = g_workload_thread_num;
    workload_threads_    = new WorkloadThread* [workload_thread_num_];
    p_workload_threads_  = new pthread_t[workload_thread_num_];

}

ThreadManager::~ThreadManager()
{
    for (uint64_t i = 0; i < txn_thread_num_; i++)
    {
        delete txn_threads_[i];
    }
    delete[] txn_threads_;
    delete[] p_txn_threads_;

    for (uint64_t i = 0; i < replay_thread_num_; i++)
    {
        delete replay_threads_[i];
    }
    delete[] replay_threads_;
    delete[] p_replay_threads_;

    for (uint64_t i = 0; i < reclaim_thread_num_; i++)
    {
        delete reclaim_threads_[i];
    }
    delete[] reclaim_threads_;
    delete[] p_reclaim_threads_;

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

    for (uint64_t i = 0; i < workload_thread_num_; i++)
    {
        delete workload_threads_[i];
    }
    delete[] workload_threads_;
    delete[] p_workload_threads_;

}



void ThreadManager::CreateTxnThread()
{
    for (uint64_t thread_id = 0; thread_id < txn_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + txn_thread_id_offset + 1;
        ProcID process_id = thread_id + txn_thread_id_offset;
        // ProcID process_id = thread_id + 24;
        if (process_id >= 48)
        {
            process_id += 24;
        }

        txn_threads_[thread_id] = new TxnThread(thread_id + txn_thread_id_offset, thread_id, process_id);
        
        printf("create txn thread, thread id: %d \n", thread_id + txn_thread_id_offset);

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


void ThreadManager::CreateReplayThread()
{
    for (uint64_t thread_id = 0; thread_id < replay_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + replay_thread_id_offset + 1;
        ProcID process_id = thread_id + replay_thread_id_offset;
        if (process_id >= 48)
        {
            process_id += 24;
        }

        replay_threads_[thread_id] = new ReplayThread(thread_id + replay_thread_id_offset, thread_id, process_id);
        
        printf("create replay thread, thread id: %d \n", thread_id + replay_thread_id_offset);

        pthread_create( &p_replay_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((ReplayThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)replay_threads_[thread_id] );
    }
}


void ThreadManager::JoinReplayThread()
{
    printf("wait replay threads finishing! \n");

    for (uint64_t i = 0; i < replay_thread_num_; i++)
    {
        pthread_join(p_replay_threads_[i], NULL);
    }

    printf("replay threads finishing! \n");
}


void ThreadManager::CreateReclaimThread()
{
    for (uint64_t thread_id = 0; thread_id < reclaim_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + reclaim_thread_id_offset + 1;
        ProcID process_id = thread_id + reclaim_thread_id_offset;
        if (process_id >= 48)
        {
            process_id += 24;
        }
    
        reclaim_threads_[thread_id] = new ReclaimThread(thread_id + reclaim_thread_id_offset, thread_id, process_id);
        
        printf("create reclaim thread, thread id: %d \n", thread_id + reclaim_thread_id_offset);

        pthread_create( &p_reclaim_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((ReclaimThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)reclaim_threads_[thread_id] );
    }
}


void ThreadManager::JoinReclaimThread()
{
    printf("wait reclaim threads finishing! \n");

    for (uint64_t i = 0; i < reclaim_thread_num_; i++)
    {
        pthread_join(p_reclaim_threads_[i], NULL);
    }

    printf("reclaim threads finishing! \n");
}

#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

void ThreadManager::CreateApplyThread()
{
    for (uint64_t thread_id = 0; thread_id < apply_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + apply_thread_id_offset + 1;
        ProcID process_id = thread_id + apply_thread_id_offset;
        if (process_id >= 48)
        {
            process_id += 24;
        }

        apply_threads_[thread_id] = new ApplyWriteThread(thread_id + apply_thread_id_offset, thread_id, process_id);

        printf("create apply write thread, thread id: %d \n", thread_id + apply_thread_id_offset);

        pthread_create( &p_apply_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((ApplyWriteThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)apply_threads_[thread_id] );
    }
}


void ThreadManager::JoinApplyThread()
{
    printf("wait apply threads finishing! \n");

    for (uint64_t i = 0; i < apply_thread_num_; i++)
    {
        pthread_join(p_apply_threads_[i], NULL);
    }

    printf("apply threads finishing! \n");
}

#endif


void ThreadManager::CreateLoggerThread()
{

    for (uint64_t thread_id = 0; thread_id < logger_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + logger_thread_id_offset + 1;
        ProcID process_id = thread_id + logger_thread_id_offset;
        // ProcID process_id = thread_id + g_txn_thread_num + g_workload_thread_num + 24;
        if (process_id >= 48)
        {
            process_id += 24;
        }

        logger_threads_[thread_id] = new LoggerThread(thread_id + logger_thread_id_offset, thread_id, process_id);

        printf("create logger thread, thread id: %d \n", thread_id + logger_thread_id_offset);
        
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
        // ProcID process_id = thread_id + replica_thread_id_offset + 1;
        ProcID process_id = thread_id + replica_thread_id_offset;

        if (process_id >= 48)
        {
            process_id += 24;
        }

        replicate_threads_[thread_id] = new ReplicateThread(thread_id + replica_thread_id_offset, thread_id, process_id);
        
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


void ThreadManager::CreateWorkloadThread()
{
    for (uint64_t thread_id = 0; thread_id < workload_thread_num_; thread_id++)
    {
        // ProcID process_id = thread_id + workload_thread_id_offset + 1;
        ProcID process_id = thread_id + workload_thread_id_offset;
        // ProcID process_id = thread_id + g_txn_thread_num + 24;

        if (process_id >= 48)
        {
            process_id += 24;
        }

        workload_threads_[thread_id] = new WorkloadThread(thread_id + workload_thread_id_offset, thread_id, process_id);
        
        printf("create workload thread, thread id: %d \n", thread_id + workload_thread_id_offset);

        pthread_create( &p_workload_threads_[thread_id], 
                        NULL, 
                        [](void *arg)
                            {
                                ((WorkloadThread*)arg)->Run();
                                return (void*)0;
                            },
                        (void *)workload_threads_[thread_id] );
    }
}


void ThreadManager::JoinWorkloadThread()
{
    printf("wait workload threads finishing! \n");

    for (uint64_t i = 0; i < workload_thread_num_; i++)
    {
        pthread_join(p_workload_threads_[i], NULL);
    }

    printf("workload threads finishing! \n");
}



DBThread* ThreadManager::GetDBThread(DBThreadType thread_type, ThreadID thread_id)
{
    DBThread* db_thread = nullptr;

    switch (thread_type)
    {
#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    case DBThreadType::APPLY_WRITE_THREAD_T:
        {
            if (thread_id >= apply_thread_num_)
            {
                printf("apply_thread_id beyond apply_thread_num, in ThreadManager::GetDBThread!\n");
                exit(0);
            }
            
            db_thread = apply_threads_[thread_id];
        }
        break;
#endif

    default:
        printf("error! non-existing thread type! in ThreadManager::GetDBThread \n");
        break;
    }

    return db_thread;
}
