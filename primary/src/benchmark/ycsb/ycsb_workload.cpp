#include "ycsb_workload.h"

#include "ycsb_schema.h"
#include "ycsb_util.h"

#include "global.h"

#include "util_function.h"

#include "index.h"
#include "tuple.h"

#include "txn_id.h"
#include "txn_context.h"

#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <math.h>
#include <set>


#if   WORKLOAD_TYPE == YCSB_W



/********** YCSBWorkload **********/
YCSBWorkload::YCSBWorkload()
{
    txn_thread_num_     = g_txn_thread_num;
    txn_num_per_thread_ = g_txns_num_per_thread;

    txns_per_threads_ = new std::queue<Transaction*>[txn_thread_num_];
}


YCSBWorkload::~YCSBWorkload()
{
    
}


Transaction* YCSBWorkload::GenTxn()
{
    YCSBTransaction* txn = new YCSBTransaction(YCSBTxnType::YCSB_TXN);
    return txn;
}


void YCSBWorkload::InsertNewTxn(ThreadID thread_id, Transaction* new_txn)
{
    txns_per_threads_[thread_id].push(new_txn);
}



/***********  YCSBTransaction  **********/

YCSBTransaction::YCSBTransaction(YCSBTxnType txn_type){
    txn_type_ = txn_type;
    request_cnt_ = g_ycsb_request_num_per_txn;
}

YCSBTransaction::~YCSBTransaction(){
}

void YCSBTransaction::GenInputData()
{
    ThreadID thread_id = txn_identifier_->thread_id_;
    local_shard_id_ = UtilFunc::URand(1, g_ycsb_shard_num, thread_id);;

    set<uint64_t> key_set;

    for (uint64_t req_id = 0; req_id < request_cnt_; req_id++)
    {
        uint64_t rand = UtilFunc::Rand(g_ycsb_rw_total_ratio, thread_id);

        if (rand < g_ycsb_read_ratio)
        {
            requests_[req_id].type = RD;
        }
        else if (rand < g_ycsb_write_ratio)
        {
            requests_[req_id].type = WR;
        }

        uint64_t key;
        do{
            key = YCSBUtilFunc::Zipf(g_ycsb_record_num_per_shard, g_ycsb_zipf_theta, thread_id);
        }while (key_set.count(key));
        key_set.insert(key);

        requests_[req_id].key = key;
    }
}


void YCSBTransaction::GenInputData(uint64_t shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_   = true;
    local_shard_id_ = shard_id;

    set<uint64_t> key_set;
    set<uint64_t> remote_req_set;

    double rand = UtilFunc::URand(1, 1000, thread_id) / (double)1000.0;
    
    if (g_ycsb_shard_num > 1 && rand < g_ycsb_remote_ratio)
    {
        is_local_txn_    = false;
        while ((remote_shard_id_ = UtilFunc::Rand(g_ycsb_shard_num, thread_id)) == local_shard_id_)
        { ; }
    }

    for (uint64_t req_id = 0; req_id < request_cnt_; req_id++)
    {
        uint64_t rand = UtilFunc::Rand(g_ycsb_rw_total_ratio, thread_id);

        if (rand < g_ycsb_read_ratio)
        {
            requests_[req_id].type = RD;
        }
        else if (rand < g_ycsb_write_ratio)
        {
            requests_[req_id].type = WR;
        }

        uint64_t key;
        do{
            key = YCSBUtilFunc::Zipf(g_ycsb_record_num_per_shard, g_ycsb_zipf_theta, thread_id);
        }while (key_set.count(key));
        key_set.insert(key);

        requests_[req_id].key      = key;
        requests_[req_id].shard_id = local_shard_id_;
    }

    if (is_local_txn_ == false)
    {
        for (uint64_t i = 0; i < g_ycsb_remote_record_num; i++)
        {
            uint64_t rand;
            do{
                rand = UtilFunc::Rand(g_ycsb_request_num_per_txn, thread_id);
            }while (remote_req_set.count(rand));
            
            remote_req_set.insert(rand);

            requests_[rand].shard_id = remote_shard_id_;
        }
    }

}



RC YCSBTransaction::RunTxn(){
    Index*       index = nullptr;
    void*        index_attr[1];
    IndexKey     index_key;

    RC rc;

    Tuple*       origin_tuple  = nullptr;
    Tuple*       operate_tuple = nullptr;
    Tuple*       new_tuple     = nullptr;
    TupleData    tuple_data    = nullptr;
    ColumnData   column_data   = nullptr;
    YCSBRequest  req;
    
    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];



    for (uint64_t rid = 0; rid < request_cnt_; rid++)
    {
        req = requests_[rid];


        index = g_schema->GetIndex(YCSB_INDEX, req.shard_id);

        index_attr[0] = &(req.key);
        index_key = g_schema->GetIndexKey(YCSB_INDEX, index_attr);

        rc = index->IndexRead(index_key, origin_tuple);

        if (rc != RC_OK)
        {
            if (txn_identifier_->thread_id_ == 0)
            {
                printf("key: %ld\n", req.key);
                count1++;
            }
            
            return RC_ABORT;
        }

        if(req.type == RD){
            uint64_t fvalsum = 0;
            rc = txn_context_->AccessTuple(AccessType::READ_AT, YCSB_TABLE, req.shard_id, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

            if(rc == RC_ABORT)
                return RC::RC_ABORT;
        
            tuple_data = operate_tuple->GetTupleData();
        
            // g_schema->GetColumnValue(YCSBTableType::YCSB_TABLE, static_cast<YCSBTableCol>(fid), tuple_data, column_data);
            g_schema->GetColumnValue(YCSBTableType::YCSB_TABLE, YCSBTableCol::F0, tuple_data, column_data);
            char* data = (char*)column_data;
            __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[0]);
        } else {
            rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, YCSB_TABLE, req.shard_id, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
        
            if(rc == RC_ABORT)
            {
                // if (txn_identifier_->thread_id_ == 0)
                // {
                //     count2++;
                // }
                return RC::RC_ABORT;
            }
            
            tuple_data = operate_tuple->GetTupleData();

            // g_schema->GetColumnValue(YCSBTableType::YCSB_TABLE, static_cast<YCSBTableCol>(fid), tuple_data, column_data);
            g_schema->GetColumnValue(YCSBTableType::YCSB_TABLE, YCSBTableCol::F1, tuple_data, column_data);
            char* data = (char*)column_data;
            // *(uint64_t *)(&data[0]) = 0;
            data[1] = 0;
            // g_schema->SetColumnValue(YCSBTableType::YCSB_TABLE, static_cast<YCSBTableCol>(fid), tuple_data, (ColumnData)&data);
            g_schema->SetColumnValue(YCSBTableType::YCSB_TABLE, YCSBTableCol::F1, tuple_data, (ColumnData)&data);
        }
    }
    return RC::RC_COMMIT;
}


#endif