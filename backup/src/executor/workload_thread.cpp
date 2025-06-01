#include "global.h"

#include "workload_thread.h"

#include "workload.h"

#include "client.h"



WorkloadThread::WorkloadThread(ThreadID thread_id, ThreadID work_thread_id, ProcID proc_id)
{
    thread_type_  = DBThreadType::WORKLOAD_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = proc_id;

    work_thread_id_ = work_thread_id;
}

WorkloadThread::~WorkloadThread()
{
}


void WorkloadThread::Run()
{
    // Transaction* new_txn = nullptr;

    // while (g_system_state != SystemState::FINISH_STATE)
    // {
    //     for (int thread_id = 0; thread_id < g_txn_thread_num; thread_id++)
    //     {                
    //         if (g_workload->WaitingQueueIsFull(thread_id))
    //         {
    //             continue;
    //         }
            
    //         new_txn = g_workload->GenTxn(thread_id);
    //         new_txn->InitTxnIdentifier(thread_id, 0);
    //     #if THREAD_BIND_SHARD
    //         new_txn->GenInputData((thread_id + 1) % g_warehouse_num + 1);
    //     #else
    //         new_txn->GenInputData();
    //     #endif
    //         GET_CLOCK_TIME(new_txn->arrive_ct_);
            
    //         if (!g_workload->InsertNewTxn(thread_id, new_txn))
    //         {
    //             delete new_txn;
    //             new_txn = nullptr;
    //         }
    //     }
    // }


    // for (ClientID client_id = 0; client_id < g_client_total_num; client_id++)
    // {
    //     clients_[client_id] = g_workload->GetClient(client_id);
    // }

    while (g_system_state != SystemState::FINISH_STATE)
    {
        for (ClientID client_id = 0; client_id < g_client_total_num; client_id++)
        {
            // Client* client = clients_[client_id];

            if (client_id % g_workload_thread_num != work_thread_id_)
            {
                continue;
            }
            
            Client* client = g_workload->GetClient(client_id);

            if (!client->IsFree())
            {
                continue;
            }
            
            Transaction* new_txn = g_workload->GenTxn();
            new_txn->InitTxnIdentifier(client_id, 0);
        #if THREAD_BIND_SHARD
            new_txn->GenInputData((thread_id + 1) % g_warehouse_num + 1);
        #else
            new_txn->GenInputData();
        #endif
            GET_CLOCK_TIME(new_txn->arrive_ct_);
            
            client->AddNewTxn(new_txn);
            COMPILER_BARRIER
            client->SetClientState(ClientStateType::CLIENT_WAITING);
        }
    }

}

