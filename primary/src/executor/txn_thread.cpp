#include "txn_thread.h"
#include "global.h"

#include "thread_manager.h"
#include "tpcc_workload.h"

#include "tpcc_config.h"
#include "ycsb_config.h"
#include "smallbank_config.h"

#include "txn_id.h"
#include "txn_context.h"

#include "log_manager.h"
#include "serial_log.h"
#include "log_buffer.h"

#include "statistic_manager.h"



#include <cstddef>
#include <stdio.h>
#include <errno.h>


TxnThread::TxnThread(ThreadID thread_id, ProcID process_id)
{
    thread_type_  = DBThreadType::TXN_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = process_id;

    local_txn_id_ = 0;
    current_txn_  = nullptr;

    local_log_buffer_ = g_log_manager->GetLogBuffer(g_log_manager->GetLogBufID(thread_id_, thread_type_));

    txn_context_  = new TxnContext();
    txn_context_->InitContext(this, g_cc_strategy_, g_log_strategy_, local_log_buffer_);
}


TxnThread::~TxnThread()
{
    
}


void TxnThread::CommitTxn(bool wait)
{
    LogLSN persistent_lsn = 0;

#if DISTRIBUTED_LOG == true
    LogBufID remote_log_buf_id = 0;
    LogLSN   remote_persistent_lsn = 0;
#endif

    while (!pre_commit_queue_.empty())
    {
#if !NO_LOG
        Transaction* pre_commit_txn = pre_commit_queue_.front();
        persistent_lsn = local_log_buffer_->persistented_lsn_;

    #if DISTRIBUTED_LOG
        remote_log_buf_id     = pre_commit_txn->remote_log_buffer_id_;
        remote_persistent_lsn = g_log_manager->GetPersistentLSN(remote_log_buf_id);

        if (pre_commit_txn->last_log_lsn_ < persistent_lsn &&
            pre_commit_txn->remote_last_log_lsn_ < remote_persistent_lsn)
    #else
        if (pre_commit_txn->last_log_lsn_ < persistent_lsn)
    #endif
        {
            //事务的日志已经被持久化
            pre_commit_queue_.pop();
            delete pre_commit_txn;
        }
        else
        {
            if (!wait)  
            {
                //队列中的事务其他预提交事务的log_lsn_一定大于persistent_lsn
                break;
            }
            
            //需要等待日志持久化
            PAUSE
            continue;
        }
#else
        Transaction* pre_commit_txn = pre_commit_queue_.front();
        pre_commit_queue_.pop();
        delete pre_commit_txn;
#endif

    }
}


void TxnThread::ExecuteWorkload(uint64_t executed_txn_num, SystemState sys_state)
{
    uint64_t commit_count = 0;
    uint64_t abort_count  = 0;

    for (uint64_t  i = 0; i < executed_txn_num; i++)
    {
        RC rc = RC::RC_COMMIT;

        // if (abort_queue_.size() > 10)
        // {
        //     //获取下一个待执行事务
        //     current_txn_ = abort_queue_.front();
        //     abort_queue_.pop();
        // }
        // else
        // {
        //     //获取下一个待执行事务
        //     current_txn_ = g_workload->GetNextTxn(thread_id_);
        // }
            //获取下一个待执行事务
            current_txn_ = g_workload->GetNextTxn(thread_id_);

        //对事务和事务上下文进行初始化
        txn_context_->InitTxn(current_txn_);

        //开始执行事务
        rc = txn_context_->RunTxn();

        //根据运行结果，结束事务
        rc = txn_context_->FinishTxn(rc);

        //重置事务上下文，用于下一个待执行事务
        txn_context_->ResetContext();




    #if PRE_COMMIT_TXN

        if (i % 100)    //每处理100个事务，提交一批预提交事务
            CommitTxn(false);
        
        current_txn_ = nullptr;

    #else
        // if (rc == RC_ABORT)
        // {
            delete current_txn_;
            current_txn_ = nullptr; 
        // }
    #endif
        if (rc == RC_COMMIT)
            commit_count++;
        else if (rc == RC_ABORT)
        {
            abort_count++;
            // abort_queue_.push(current_txn_);
        }

        if ( i%100 == 0 && g_system_state == SystemState::TESTING_STATE)
        {
            g_statistic_manager->TxnCommit(thread_id_ - txn_thread_id_offset, commit_count);
            g_statistic_manager->TxnAbort(thread_id_ - txn_thread_id_offset, abort_count);
            commit_count = 0;
            abort_count = 0;
        }
        
        /** think time! **/
        uint64_t count = 0;
        for (uint64_t j = 0; j < g_think_time; j++)
        {
            count++;
        }
    }
}




void TxnThread::Run()
{

#if THREAD_BIND_CORE == true
        SetAffinity();
#endif


    for (uint64_t local_txn_id = 0; local_txn_id < g_txns_num_per_thread; local_txn_id++)
    {
        Transaction* new_txn = g_workload->GenTxn();
        new_txn->InitTxnIdentifier(thread_id_, local_txn_id);

#if THREAD_BIND_SHARD
    uint64_t shard_id = 0;

    #if   WORKLOAD_TYPE == TPCC_W
        shard_id = (thread_id_ - txn_thread_id_offset) % g_warehouse_num + 1;
        new_txn->GenInputData(shard_id);
    #elif WORKLOAD_TYPE == YCSB_W
        // shard_id = (thread_id_ - txn_thread_id_offset) % g_ycsb_shard_num + (local_txn_id % (g_ycsb_shard_num / g_txn_thread_num)) * g_txn_thread_num;
        shard_id = (thread_id_ - txn_thread_id_offset) % g_ycsb_shard_num;
        new_txn->GenInputData(shard_id);

    #elif WORKLOAD_TYPE == SMALLBANK_W
        // shard_id = (thread_id_ - txn_thread_id_offset) % g_ycsb_shard_num + (local_txn_id % (g_ycsb_shard_num / g_txn_thread_num)) * g_txn_thread_num;
        shard_id = (thread_id_ - txn_thread_id_offset) % g_smallbank_shard_num;
        new_txn->GenInputData(shard_id);

    #endif
#else  
        new_txn->GenInputData();
#endif

        g_workload->InsertNewTxn((thread_id_ - txn_thread_id_offset), new_txn);
    }

    printf("事务线程: %ld 生成 %ld 个待执行事务!\n", thread_id_ - txn_thread_id_offset, g_txns_num_per_thread);


    printf("Thread %d waits EXCUTEWORKLOAD! \n", thread_id_);
    pthread_barrier_wait(g_thread_manager->execute_workload_barrier_);
    printf("Thread %d starts EXECUTEWORKLOAD! \n", thread_id_);


    /* 系统预热阶段 */
    ExecuteWorkload(g_warmup_txns_num_per_thread, SystemState::WARMUP_STATE);

    if (ATOM_SUB_FETCH(g_workload->warmup_thread_num_, 1) == 0)
    {
        g_workload->warmup_finish_ = true;
    }

    /* 系统执行阶段，采样性能数据 */
    ExecuteWorkload(g_test_txns_num_per_thread, SystemState::TESTING_STATE);

    g_workload->testing_finish_ = true;


    /* 系统冷却阶段 */
    ExecuteWorkload(g_cooldown_txns_num_per_thread, SystemState::COOLDOWN_STATE);


#if PRE_COMMIT_TXN
    CommitTxn(false);
#endif

    /* 系统结束负载执行 */
    if (ATOM_SUB_FETCH(g_workload->working_thread_num_, 1) == 0)
    {
        g_workload->workload_finish_ = true;
    }


    printf("Thread %d finishs EXCUTEWORKLOAD! \n", thread_id_);
}

