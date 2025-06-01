#include "ycsb_workload.h"

#include "global.h"

#include "util_function.h"

#include "ycsb_schema.h"

#include "index.h"
#include "tuple.h"

#include "txn_id.h"
#include "txn_context.h"

#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <math.h>
#include <set>

using namespace std;


#if   WORKLOAD_TYPE == YCSB_W


/******** TPCCWorkload ********/

YCSBWorkload::YCSBWorkload()
{
    // txn_thread_num_   = g_txn_thread_num;
    // txns_per_threads_ = new std::queue<Transaction*>[txn_thread_num_];
}


YCSBWorkload::~YCSBWorkload()
{

}


Transaction* YCSBWorkload::GenTxn()
{
    Transaction* txn  = new YCSBTransaction(YCSBTxnType::YCSB_TXN);
    return txn;
}


// Transaction* YCSBWorkload::GenTxn(ThreadID thread_id)
// {
//     Transaction* txn  = new YCSBTransaction(YCSBTxnType::YCSB_TXN);
//     return txn;
// }

// bool YCSBWorkload::InsertNewTxn(ThreadID thread_id, Transaction* new_txn)
// {
//     waiting_queue_lock_[thread_id].GetSpinLock();

//     if (txns_per_threads_[thread_id].empty() || txns_per_threads_[thread_id].size() < g_waiting_txn_queue_capacity)
//     {
//         txns_per_threads_[thread_id].push(new_txn);
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return true;
//     }
//     else
//     {
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return false;
//     }
// }




/***********  YCSBTransaction  **********/

YCSBTransaction::YCSBTransaction(YCSBTxnType txn_type){
    txn_type_ = txn_type;
}

YCSBTransaction::~YCSBTransaction(){
}

void YCSBTransaction::GenInputData()
{
    ClientID client_id = txn_identifier_->GetClientId();
    shard_id_  = UtilFunc::URand(0, g_ycsb_shard_num - 1, client_id);
    start_key_ = UtilFunc::Zipf(g_ycsb_record_num - g_ycsb_scan_num, g_ycsb_zipf_theta, client_id);
    scan_cnt_  = g_ycsb_scan_num;
}


void YCSBTransaction::GenInputData(uint64_t shard_id)
{
    ClientID client_id = txn_identifier_->GetClientId();

    shard_id_  = shard_id;
    start_key_ = UtilFunc::Zipf(g_ycsb_record_num - g_ycsb_scan_num, g_ycsb_zipf_theta, client_id);
    scan_cnt_  = g_ycsb_scan_num;
}



RC YCSBTransaction::RunTxn(){
    Index*       index = nullptr;
    void*        index_attr[1];
    IndexKey     index_key;

    RC rc;

    AccessObj* read_ao     = nullptr;

    char       tuple_data[MAX_TUPLE_SIZE];
    ColumnData column_data = nullptr;

    index = g_schema->GetIndex(YCSB_INDEX, shard_id_);
    for (uint64_t key = start_key_; key < start_key_ + scan_cnt_; key++)
    {
        index_attr[0] = &key;
        index_key = g_schema->GetIndexKey(YCSB_INDEX, index_attr);

        rc = index->IndexRead(index_key, read_ao);

        if (rc != RC_OK)
        {
            if (txn_identifier_->GetClientId() == 0)
            {
                printf("key: %ld\n", key);
                count4++;
            }
            
            return RC_ABORT;
        }

        txn_context_->AccessTuple(read_ao, YCSBTableType::YCSB_TABLE, shard_id_, index_key, tuple_data);

        //READ
        g_schema->GetColumnValue(YCSB_TABLE, YCSBTableCol::F0, tuple_data, column_data);
        char* data = (char*)column_data;
        __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[0]);
    }

    return RC::RC_COMMIT;
}


#endif