#include "tuple_ver_chain_am.h"

#include "schema.h"
#include "tpcc_schema.h"

#include "index.h"
#include "tuple.h"

#include "log_strategy.h"
#include "serial_log.h"
#include "log_replay_context.h"

#include "txn_context.h"

#include "statistic_manager.h"

#include "db_rw_lock.h"
#include "db_spin_lock.h"

#include <string.h>


#if  AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM


/*************** TupVerChainAO *************/

TupleVerChainAO::TupleVerChainAO()
{
    latest_version_ = nullptr;
    deleted_ = false;
}


void TupleVerChainAO::InsertNewVersion(Tuple* new_tuple_ver, bool deleted)
{
    spin_lock_.GetSpinLock();

    Timestamp visible_ts = new_tuple_ver->visible_ts_;

    deleted_ = deleted;

    Tuple* next_ver = latest_version_;
    Tuple* pre_ver  = nullptr;

    while (next_ver != nullptr)
    {
        if (visible_ts < next_ver->visible_ts_)
        {
            pre_ver = next_ver;
            next_ver = next_ver->next_tuple_ver_;
        }
        else
        {
            break;
        }
    }
    
    new_tuple_ver->next_tuple_ver_ = next_ver;

    COMPILER_BARRIER

    if (pre_ver != nullptr)
    {
        pre_ver->next_tuple_ver_ = new_tuple_ver;
    }
    else
    {
        latest_version_ = new_tuple_ver;
    }

    spin_lock_.ReleaseSpinLock();
}



/*************** TupleVerChainAM *************/

TupleVerChainAM::TupleVerChainAM()
{

}

TupleVerChainAM::~TupleVerChainAM()
{

}


bool TupleVerChainAM::GetTupleData(AccessObj* access_obj, 
                                   TableID table_id, 
                                   TupleData tuple_data, 
                                   Timestamp visible_ts, 
                                   ThreadID replay_thread_id)
{
    TupleVerChainAO* tuple_ver_chain_ao = dynamic_cast<TupleVerChainAO*>(access_obj);

    if (tuple_ver_chain_ao->deleted_ && tuple_ver_chain_ao->latest_version_->visible_ts_ <= visible_ts)
    {
        return false;
    }
    
    Tuple* current_tuple = tuple_ver_chain_ao->latest_version_;

    while (current_tuple != nullptr)
    {
        if (current_tuple->visible_ts_ <= visible_ts)
        {
            memcpy(tuple_data, current_tuple->tuple_data_, g_schema->GetTupleSize(table_id)); 
            break;
        }
        current_tuple = current_tuple->next_tuple_ver_;
    }

    if (current_tuple == nullptr)
    {
        return false;
    }

    return true;
}

bool TupleVerChainAM::GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      TxnContext* txn_context,
                      ShardID    shard_id,
                      PrimaryKey primary_key)
{
    TupleVerChainAO* tuple_ver_chain_ao = dynamic_cast<TupleVerChainAO*>(access_obj);

    Timestamp visible_ts = txn_context->GetReadTS();

    if (tuple_ver_chain_ao->deleted_ && tuple_ver_chain_ao->latest_version_->visible_ts_ <= visible_ts)
    {
        return false;
    }
    
    Tuple* current_tuple = tuple_ver_chain_ao->latest_version_;

    while (current_tuple != nullptr)
    {
        if (current_tuple->visible_ts_ <= visible_ts)
        {
            memcpy(tuple_data, current_tuple->tuple_data_, g_schema->GetTupleSize(table_id)); 
            break;
        }
        current_tuple = current_tuple->next_tuple_ver_;
    }

    if (current_tuple == nullptr)
    {
        return false;
    }

    return true;
}


bool TupleVerChainAM::ReplayTupleLog(LogTupleMeta* log_tuple_meta, LogReplayContext* replay_context)
{
    RC rc = RC_OK;
    AccessObj*       access_obj = nullptr;
    TupleVerChainAO* tuple_ao   = nullptr;

    Index* primary_index = g_schema->GetIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);


#if    TIME_BREAKDOWN_ANALYTIC && USING_FA_REPLAY_CREATE_VERSION_TIME_T
    
    ClockTime  create_version_begin_time_ = 0;
    ClockTime  create_version_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(create_version_begin_time_);
    }
#endif



    Tuple* new_tuple_ver = nullptr;
    switch (log_tuple_meta->opt_type_)
    {
    case UPDATE_AT:
    case INSERT_AT:
            new_tuple_ver = new Tuple(g_schema->GetTupleSize(log_tuple_meta->table_id_), log_tuple_meta->commit_ts_);
            new_tuple_ver->CopyTupleData((TupleData)log_tuple_meta->tuple_log_ptr_, log_tuple_meta->tuple_log_size_);
            break;
    case DELETE_AT:
            new_tuple_ver = new Tuple(0, log_tuple_meta->commit_ts_);
            break;
    default:
        break;
    }


#if    TIME_BREAKDOWN_ANALYTIC && USING_FA_REPLAY_CREATE_VERSION_TIME_T
    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(create_version_end_time_);
        g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                REPLAY_THREAD_T, 
                                                FA_REPLAY_CREATE_VERSION_TIME_T, 
                                                create_version_end_time_ - create_version_begin_time_);
    }
#endif


#if    MICRO_STATISTIC_ANALYTIC && USING_EA_REPLAY_CREATE_VERSION_COUNT_T
    if (replay_context->IsMicroStatisticSample())
    {
        g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, EA_REPLAY_CREATE_VERSION_COUNT_T, 1);
    }
#endif



#if    TIME_BREAKDOWN_ANALYTIC && USING_EALA_REPLAY_ACCESS_KV_INDEX_TIME_T

    ClockTime  access_kv_index_begin_time_ = 0;
    ClockTime  access_kv_index_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(access_kv_index_begin_time_);
    }
#endif



    rc = primary_index->IndexRead(log_tuple_meta->primary_key_, access_obj);


    if (rc == RC_OK)
    {
        tuple_ao = dynamic_cast<TupleVerChainAO*>(access_obj);
        tuple_ao->InsertNewVersion(new_tuple_ver, log_tuple_meta->opt_type_ == DELETE_AT);
    
    }
    else
    {
        tuple_ao = new TupleVerChainAO();

        tuple_ao->latest_version_ = new_tuple_ver;
        if (log_tuple_meta->opt_type_ == DELETE_AT)
        {
            tuple_ao->deleted_ = true;
        }
        
        rc = primary_index->IndexInsert(log_tuple_meta->primary_key_, tuple_ao);

        if (rc == RC_ERROR)
        {
            delete tuple_ao;

            rc = primary_index->IndexRead(log_tuple_meta->primary_key_, access_obj);

            tuple_ao = dynamic_cast<TupleVerChainAO*>(access_obj);
            tuple_ao->InsertNewVersion(new_tuple_ver, log_tuple_meta->opt_type_ == DELETE_AT);
        }
        else
        {
            access_obj = tuple_ao;
        }
    }


#if    MICRO_STATISTIC_ANALYTIC && USING_EA_REPLAY_ACCESS_KV_INDEX_COUNT_T
    if (replay_context->IsMicroStatisticSample())
    {
        g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, EA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
    }
#endif


    for (size_t i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
    {
        IndexID    index_id    = log_tuple_meta->index_and_opts_[i].index_id_;
        AccessType access_type = log_tuple_meta->index_and_opts_[i].index_opt_;

        Index*  secondary_index = g_schema->GetIndex(index_id, log_tuple_meta->shard_id_);

        IndexKey     index_key = g_schema->GetIndexKey(index_id, new_tuple_ver->tuple_data_);
        PartitionKey part_key  = g_schema->GetPartKey(index_id, new_tuple_ver->tuple_data_);

        switch (access_type)
        {
        case INSERT_AT:
            secondary_index->IndexInsert(index_key, access_obj, part_key, log_tuple_meta->commit_ts_);
            break;
        case DELETE_AT:
            secondary_index->IndexRemove(index_key, part_key, log_tuple_meta->commit_ts_);
            break;
        default:
            break;
        }

    #if    MICRO_STATISTIC_ANALYTIC && USING_EA_REPLAY_ACCESS_KV_INDEX_COUNT_T
        if (replay_context->IsMicroStatisticSample())
        {
            g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, EA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
        }
    #endif

    }

#if    TIME_BREAKDOWN_ANALYTIC && USING_EALA_REPLAY_ACCESS_KV_INDEX_TIME_T
    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(access_kv_index_end_time_);
        g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                REPLAY_THREAD_T, 
                                                EALA_REPLAY_ACCESS_KV_INDEX_TIME_T, 
                                                access_kv_index_end_time_ - access_kv_index_begin_time_);
    }
#endif

    return true;
}



#endif