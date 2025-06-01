#include "txn_thread.h"
#include "global.h"

#include "thread_manager.h"
#include "tpcc_workload.h"

#include "txn_id.h"
#include "txn_context.h"

#include "client.h"

#include "workload.h"
#include "log_manager.h"
#include "serial_log.h"
#include "log_buffer.h"

#include "comm_manager.h"
#include "global_ts_comm.h"

#include "statistic_manager.h"

#include "snapshot_manager.h"



TxnThread::TxnThread(ThreadID thread_id, ThreadID txn_thread_id, ProcID process_id)
{
    thread_type_  = DBThreadType::TXN_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = process_id;

    for (uint64_t i = 0; i < g_client_num_per_txn_thread; i++)
    {
        clients_[i] = nullptr;
    }

    txn_thread_id_ = txn_thread_id;
    local_txn_id_ = 0;
    // current_txn_  = nullptr;

    // txn_context_  = new TxnContext();
    // txn_context_->InitContext(this, g_am_strategy_);

    // for (size_t i = 0; i < g_log_buffer_num; i++)
    // {
    //     LogBuffer* log_buf = g_log_manager->GetLogBuffer(i);
    //     log_buf->txn_contexts_[txn_thread_id_] = txn_context_;
    // }
}


TxnThread::~TxnThread()
{
    
}



// void TxnThread::ExecuteWorkload(SystemState system_state)
// {

//     GlobalTSComm* global_ts_comm = g_comm_manager->global_ts_comm_;

//     uint64_t count = 0;

//     while (system_state == g_system_state)
//     {
//         current_txn_ = g_workload->GetNextTxn(txn_thread_id_);
//         while (current_txn_ == nullptr && system_state == g_system_state)
//         {
//             if (system_state != g_system_state)
//                 goto Finish;
            
//             current_txn_ = g_workload->GetNextTxn(txn_thread_id_);
//         }

//         //获取只读一致时间戳
//         ClockTime fetch_global_cts_start = 0;
//         GET_CLOCK_TIME(fetch_global_cts_start);

//         g_snapshot_manager->BeforeFetchCTS(txn_thread_id_);

//         current_txn_->read_ts_ = global_ts_comm->FetchGlobalCTS(current_txn_->arrive_ct_);

//         // g_snapshot_manager->visible_ts_[txn_thread_id_] = current_txn_->read_ts_;
//         g_snapshot_manager->AfterFetchCTS(txn_thread_id_, current_txn_->read_ts_);


//         ClockTime fetch_global_cts_finish = 0;
//         GET_CLOCK_TIME(fetch_global_cts_finish);

//         txn_context_->InitTxn(current_txn_, current_txn_->read_ts_);

//         //保证备节点已经达到只读事务可见的一致状态
//         ClockTime wait_consistent_snapshot_start = 0;
//         GET_CLOCK_TIME(wait_consistent_snapshot_start);

//         while (current_txn_->read_ts_ > g_visible_ts)
//         {
//             PAUSE
//             if (system_state != g_system_state)
//                 goto Finish;
//         }

//         ClockTime wait_consistent_snapshot_finish = 0;
//         GET_CLOCK_TIME(wait_consistent_snapshot_finish);


//         //模拟执行事务负载
//         ClockTime execute_txn_start = 0;
//         GET_CLOCK_TIME(execute_txn_start);

//     #if    SIMULATE_WORKLOAD  == true
//         int count = 0;
//         for (uint64_t i = 0; i < g_think_time; i++)
//         {
//             count++;
//         }
//     #else 
//         txn_context_->RunTxn();
//     #endif

//         ClockTime execute_txn_finish = 0;
//         GET_CLOCK_TIME(execute_txn_finish);

//         //事务提交
//         if (system_state == SystemState::TESTING_STATE)
//         {
//             g_statistic_manager->TxnCommit(txn_thread_id_);
//             g_statistic_manager->TxnFetchCTSTime(txn_thread_id_, fetch_global_cts_finish - fetch_global_cts_start);
//             g_statistic_manager->TxnWaitConsistentSnapshot(txn_thread_id_, wait_consistent_snapshot_finish - wait_consistent_snapshot_start);
//             g_statistic_manager->TxnExecuteTime(txn_thread_id_, execute_txn_finish - execute_txn_start);
//         }

//         txn_context_->ResetContext();

//         delete current_txn_;
//         current_txn_ = nullptr;
//     }
    
// Finish:
//     return;

// }


void TxnThread::ResetTxnThread()
{
    local_txn_id_ = 1;
    // current_txn_  = nullptr;
}


void TxnThread::Run()
{
#if THREAD_BIND_CORE
        SetAffinity();
#endif


    AssociateClient();

    printf("事务线程 %ld 开始执行负载!\n", thread_id_ - txn_thread_id_offset);
    

    // /* 系统预热阶段 */
    // printf("事务线程 %ld 进入warmup状态!\n", thread_id_ - txn_thread_id_offset);
    // ExecuteWorkload(SystemState::WARMUP_STATE);

    // /* 系统执行阶段，采样性能数据 */
    // g_statistic_manager->ThreadStart(thread_id_ - txn_thread_id_offset);
    // printf("事务线程 %ld 进入testing状态!\n", thread_id_ - txn_thread_id_offset);
    // ExecuteWorkload(SystemState::TESTING_STATE);
    // g_statistic_manager->ThreadEnd(thread_id_ - txn_thread_id_offset);

    // /* 系统冷却阶段 */
    // printf("事务线程 %ld 进入cooldown状态!\n", thread_id_ - txn_thread_id_offset);
    // ExecuteWorkload(SystemState::COOLDOWN_STATE);

    // printf("事务线程 %ld 结束负载!\n", thread_id_ - txn_thread_id_offset);



    while (g_system_state != SystemState::FINISH_STATE)
    {
        for (uint64_t i = 0; i < g_client_num_per_txn_thread; i++)
        {
            Client*     client      = clients_[i];
            TxnContext* txn_context = client->GetTxnContext();

            RC rc = txn_context->ExecuteTxn();
        }
    }

}



void TxnThread::AssociateClient()
{
    for (uint64_t i = 0; i < g_client_num_per_txn_thread; i++)
    {
        clients_[i] = g_workload->GetClient(txn_thread_id_ * g_client_num_per_txn_thread + i);

        clients_[i]->AssociateTxnThread(this);
    }
}


ThreadID TxnThread::GetTxnThreadID()
{
    return txn_thread_id_;
}