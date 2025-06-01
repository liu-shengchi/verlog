#include "workload.h"
#include "global.h"

#include "txn_id.h"

#include <malloc.h>


/******* Workload ********/

Workload::Workload()
{
    warmup_thread_num_  = g_txn_thread_num;
    warmup_finish_      = false;

    testing_finish_     = false;

    working_thread_num_ = g_txn_thread_num;
    workload_finish_    = false;
}


Workload::~Workload()
{

}


Transaction* Workload::GetNextTxn(uint64_t  thread_id)
{
    Transaction* next_txn = txns_per_threads_[thread_id].front();
    txns_per_threads_[thread_id].pop();

    return next_txn;
}



/****** Transaction ******/

Transaction::Transaction(ThreadID thread_id, uint64_t local_txn_id)
{
    txn_identifier_ = new TxnIdentifier(thread_id, local_txn_id);
}

Transaction::~Transaction()
{
    delete txn_identifier_;
}


void Transaction::InitTxnIdentifier(ThreadID thread_id, uint64_t local_txn_id)
{
    txn_identifier_ = new TxnIdentifier(thread_id, local_txn_id);
}


TxnIdentifier* Transaction::GetTxnIdentifier()
{ 
    return txn_identifier_;
}


void Transaction::SetTxnContext(TxnContext* txn_context)
{
    txn_context_ = txn_context;
}


