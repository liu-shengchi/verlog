#include "workload.h"
#include "global.h"

#include "txn_id.h"
#include "client.h"

#include "db_spin_lock.h"

#include <malloc.h>


/******* Workload ********/

Workload::Workload()
{
    // working_thread_num_ = g_txn_thread_num;
    // workload_finish_    = false;

    
    for (uint64_t client_id = 0; client_id < g_client_total_num; client_id++)
    {
        clients_[client_id] = new Client(client_id);
    }
}


Workload::~Workload()
{

}


// Transaction* Workload::GetNextTxn(uint64_t thread_id)
// {
//     waiting_queue_lock_[thread_id].GetSpinLock();
    
//     Transaction* next_txn = nullptr;

//     if (!txns_per_threads_[thread_id].empty())
//     {
//         next_txn = txns_per_threads_[thread_id].front();
//         txns_per_threads_[thread_id].pop();        
//     }
    
//     waiting_queue_lock_[thread_id].ReleaseSpinLock();
    
//     return next_txn;
// }

// bool Workload::WaitingQueueIsFull(ThreadID thread_id)
// {
//     waiting_queue_lock_[thread_id].GetSpinLock();

//     if (txns_per_threads_[thread_id].empty() || txns_per_threads_[thread_id].size() < g_waiting_txn_queue_capacity)
//     {
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return false;
//     }
//     else
//     {
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return true;
//     }
// }



// void Workload::AppendNewTxn(ClientID client_id, Transaction* new_txn)
// {
//     clients_[client_id]->AppendNewTxn(new_txn);
// }


Client* Workload::GetClient(ClientID client_id)
{
    return clients_[client_id];
}



/****** Transaction ******/


// Transaction::Transaction(ThreadID thread_id, uint64_t local_txn_id)
// {
//     txn_identifier_ = new TxnIdentifier(thread_id, local_txn_id);
// }

Transaction::Transaction()
{
    txn_identifier_ = new TxnIdentifier(0, 0);
}

Transaction::~Transaction()
{
    delete txn_identifier_;
}


void Transaction::InitTxnIdentifier(ClientID client_id, uint64_t local_txn_id)
{
    txn_identifier_->SetTxnId(client_id, local_txn_id);
}


TxnIdentifier* Transaction::GetTxnIdentifier()
{ 
    return txn_identifier_;
}


void Transaction::SetTxnContext(TxnContext* txn_context)
{
    txn_context_ = txn_context;
}


