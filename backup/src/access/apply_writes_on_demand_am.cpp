#include "apply_writes_on_demand_am.h"

#include "thread_manager.h"
#include "apply_write_thread.h"

#include "schema.h"

#include "index.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "log_strategy.h"
#include "logindex_log.h"
#include "log_replay_context.h"

#include "txn_context.h"
#include "txn_thread.h"

#include "statistic_manager.h"

#include "string.h"



#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM


WriteInfo::WriteInfo(Timestamp commit_ts, LogLogicalPtr log_ptr, LogSize log_size)
{
    commit_ts_ = commit_ts;
    log_ptr_   = log_ptr;
    log_size_  = log_size;

    is_applying_     = false;
    is_applied_      = false;
    next_write_info_ = nullptr;
}


WriteInfoAO::WriteInfoAO()
{
    latest_write_info_ = nullptr;
}

void WriteInfoAO::InsertNewWriteInfo(WriteInfo* new_wi)
{
    spin_lock_.GetSpinLock();

    Timestamp commit_ts = new_wi->commit_ts_;

    WriteInfo* next_wi = latest_write_info_;
    WriteInfo* prev_wi  = nullptr;

    while (next_wi != nullptr)
    {
        if (commit_ts < next_wi->commit_ts_)
        {
            prev_wi = next_wi;
            next_wi = next_wi->next_write_info_;
        }
        else
        {
            break;
        }
    }
    
    new_wi->next_write_info_ = next_wi;

    COMPILER_BARRIER

    if (prev_wi != nullptr)
    {
        prev_wi->next_write_info_ = new_wi;
    }
    else
    {
        latest_write_info_ = new_wi;
    }

    spin_lock_.ReleaseSpinLock();
}


ApplyWritesOnDemandAO::ApplyWritesOnDemandAO()
{
    latest_version_ = nullptr;
}


void ApplyWritesOnDemandAO::InsertNewVersion(Tuple* new_ver)
{
    spin_lock_.GetSpinLock();

    Timestamp visible_ts = new_ver->visible_ts_;

    Tuple* next_ver = latest_version_;
    Tuple* prev_ver  = nullptr;

    while (next_ver != nullptr)
    {
        if (visible_ts < next_ver->visible_ts_)
        {
            prev_ver = next_ver;
            next_ver = next_ver->next_tuple_ver_;
        }
        else
        {
            break;
        }
    }
    
    new_ver->next_tuple_ver_ = next_ver;

    COMPILER_BARRIER

    if (prev_ver != nullptr)
    {
        prev_ver->next_tuple_ver_ = new_ver;
    }
    else
    {
        latest_version_ = new_ver;
    }

    spin_lock_.ReleaseSpinLock();
}



ApplyWritesOnDemandAM::ApplyWritesOnDemandAM()
{

}
ApplyWritesOnDemandAM::~ApplyWritesOnDemandAM()
{

}


bool ApplyWritesOnDemandAM::GetTupleData(AccessObj* access_obj, 
                                         TableID table_id, 
                                         TupleData tuple_data, 
                                         Timestamp visible_ts, 
                                         ThreadID replay_thread_id)
{


    return true;
}


bool ApplyWritesOnDemandAM::GetTupleData(AccessObj*  access_obj, 
                                         TableID     table_id, 
                                         TupleData   tuple_data, 
                                         TxnContext* txn_context,
                                         ShardID     shard_id,
                                         PrimaryKey  primary_key)
{
    ApplyWritesOnDemandAO* kv_ao = dynamic_cast<ApplyWritesOnDemandAO*>(access_obj);

    Timestamp visible_ts = txn_context->GetReadTS();

    Tuple* visible_ver = kv_ao->latest_version_;

    while (visible_ver != nullptr)
    {
        if (visible_ver->visible_ts_ <= visible_ts)
        { 
            break;
        }
        visible_ver = visible_ver->next_tuple_ver_;
    }

#if    TIME_BREAKDOWN_ANALYTIC
    
    ClockTime  fetch_unapplied_write_info_begin_time_ = 0;
    ClockTime  fetch_unapplied_write_info_end_time_   = 0;

    ClockTime  load_log_from_logstore_begin_time_ = 0;
    ClockTime  load_log_from_logstore_end_time_   = 0;

    ClockTime  create_version_begin_time_ = 0;
    ClockTime  create_version_end_time_   = 0;


    if (txn_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(fetch_unapplied_write_info_begin_time_);
    }
#endif

    Index* unapplied_index = g_schema->GetUnappliedIndex(table_id, shard_id);

    AccessObj*   wi_ao         = nullptr;
    WriteInfoAO* wi_access_obj = nullptr;

    unapplied_index->IndexRead(primary_key, wi_ao);

    if (wi_ao == nullptr)
    {
        printf("error!, wi_ao can not be null! ApplyWritesOnDemandAM::GetTupleData  \n");
        exit(0);
    }

    wi_access_obj = dynamic_cast<WriteInfoAO*>(wi_ao);

    WriteInfo* visible_write = wi_access_obj->latest_write_info_;
    while (visible_write != nullptr)
    {
        if (visible_write->commit_ts_ <= visible_ts)
        {
            break;
        }
        visible_write = visible_write->next_write_info_;
    }


#if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T
    if (txn_context->IsMicroStatisticSample())
    {
        g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T, 1);
    }
#endif


#if    TIME_BREAKDOWN_ANALYTIC
    if (txn_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(fetch_unapplied_write_info_end_time_);

        g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T, fetch_unapplied_write_info_end_time_ - fetch_unapplied_write_info_begin_time_);
    }
#endif


#if    APPLY_WRITE_DURING_EXECUTING


    if (visible_ver == nullptr && visible_write == nullptr)
    {
        printf("error! version && unapplied write both are equal to null! ApplyWritesOnDemandAM::GetTupleData \n");
        exit(0);
    }
    else if (visible_write == nullptr)
    {
        printf("error! unapplied write cann't are equal to null! ApplyWritesOnDemandAM::GetTupleData \n");
        exit(0);
    }
    else if (visible_ver == nullptr || visible_write->commit_ts_ > visible_ver->visible_ts_)
    {

    #if    TIME_BREAKDOWN_ANALYTIC
        if (txn_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(create_version_begin_time_);
        }
    #endif

        visible_write->lock_.GetLatch();

        if (visible_write->is_applied_)
        {
            visible_write->lock_.ReleaseLatch();
        }
        else if (visible_write->is_applying_)
        {
            visible_write->lock_.ReleaseLatch();
            
            while (visible_write->is_applying_)
            {
                ;
            }
        }
        else
        {
            visible_write->is_applying_ = true;
            visible_write->lock_.ReleaseLatch();

        #if    TIME_BREAKDOWN_ANALYTIC
            if (txn_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(load_log_from_logstore_begin_time_);
            }
        #endif

            LogBufID    log_buf_id = (visible_write->log_ptr_ & LOGBUF_MASK) >> LOGBUF_OFFSET;
            LogLSN      log_lsn    = (visible_write->log_ptr_ & LOGLSN_MASK) >> LOGLSN_OFFSET;
            LogSize     log_size   = visible_write->log_size_;
    
            LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);
    
            char log_entry[MAX_TUPLE_LOG_SIZE] = {'0'};
            bool in_log_buf = true;
            
            if (log_buf->free_lsn_ < log_lsn)
            {
                COMPILER_BARRIER
                memcpy(log_entry, log_buf->buffer_ + (log_lsn % log_buf->buffer_size_), log_size);
                COMPILER_BARRIER
    
                if (!(log_buf->free_lsn_ < log_lsn))
                {
                    in_log_buf = false;
                }
            }
            else
            {
                in_log_buf = false;
            }
            
        #if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_LOAD_LOG_COUNT_T
            if (txn_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_LOAD_LOG_COUNT_T, 1);
            }
        #endif

            if (!in_log_buf)
            {
                log_buf->ReadFromDurable(log_entry, log_size, log_lsn);

            #if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_ACCESS_DISK_COUNT_T
                if (txn_context->IsMicroStatisticSample())
                {
                    g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_ACCESS_DISK_COUNT_T, 1);
                }
            #endif
            }
    
        #if    TIME_BREAKDOWN_ANALYTIC
            if (txn_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(load_log_from_logstore_end_time_);
            }

            g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T, load_log_from_logstore_end_time_ - load_log_from_logstore_begin_time_);
        #endif


            Timestamp commit_ts   = visible_write->commit_ts_;
            uint64_t  tuple_size  = g_schema->GetTupleSize(table_id);
            
            Tuple*    new_version = new Tuple(tuple_size, commit_ts);
            new_version->CopyTupleData((TupleData)((char*)log_entry) + 40, tuple_size);
    
            kv_ao->InsertNewVersion(new_version);
    
        #if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_CREATE_VERSION_COUNT_T
            if (txn_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_CREATE_VERSION_COUNT_T, 1);
            }
        #endif
    
            COMPILER_BARRIER
    
            visible_write->lock_.GetLatch();
            visible_write->is_applied_  = true; 
            visible_write->is_applying_ = false;
            visible_write->lock_.ReleaseLatch();
        }


    #if    TIME_BREAKDOWN_ANALYTIC
        if (txn_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(create_version_end_time_);
            
            g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_CREATE_VERSION_TIME_T, create_version_end_time_ - create_version_begin_time_);
        }
    #endif


        Tuple* cur_ver = kv_ao->latest_version_;

        while (cur_ver != nullptr)
        {
            if (cur_ver->visible_ts_ <= visible_ts)
            {
                break;
            }
            cur_ver = cur_ver->next_tuple_ver_;
        }

        if (cur_ver == nullptr || cur_ver == visible_ver)
        {
            printf("error! apply write fail! ApplyWritesOnDemandAM::GetTupleData \n");
        }
        
        memcpy(tuple_data, cur_ver->tuple_data_, g_schema->GetTupleSize(table_id));
    }
    else
    {
        memcpy(tuple_data, visible_ver->tuple_data_, g_schema->GetTupleSize(table_id));
    }

#else


    if (visible_ver == nullptr && visible_write == nullptr)
    {
        printf("error! version && unapplied write both are equal to null! ApplyWritesOnDemandAM::GetTupleData \n");
        exit(0);
    }
    else if (visible_write == nullptr)
    {
        printf("error! unapplied write cann't are equal to null! ApplyWritesOnDemandAM::GetTupleData \n");
        exit(0);
    }
    else if (visible_ver == nullptr || visible_write->commit_ts_ > visible_ver->visible_ts_)
    {

    #if    TIME_BREAKDOWN_ANALYTIC
        if (txn_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(load_log_from_logstore_begin_time_);
        }
    #endif

        LogBufID    log_buf_id = (visible_write->log_ptr_ & LOGBUF_MASK) >> LOGBUF_OFFSET;
        LogLSN      log_lsn    = (visible_write->log_ptr_ & LOGLSN_MASK) >> LOGLSN_OFFSET;
        LogSize     log_size   = visible_write->log_size_;

        LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);

        char log_entry[MAX_TUPLE_LOG_SIZE] = {'0'};
        bool in_log_buf = true;
        
        if (log_buf->free_lsn_ < log_lsn)
        {
            COMPILER_BARRIER
            memcpy(log_entry, log_buf->buffer_ + (log_lsn % log_buf->buffer_size_), log_size);
            COMPILER_BARRIER

            if (!(log_buf->free_lsn_ < log_lsn))
            {
                in_log_buf = false;
            }
        }
        else
        {
            in_log_buf = false;
        }

        if (!in_log_buf)
        {
            log_buf->ReadFromDurable(log_entry, log_size, log_lsn);

        #if MICRO_STATISTICAL_DATA_ANALYTIC
            if (apply_write_thread_id_ == 0)
            {
                count13++;
            }
        #endif            
        }

    #if    TIME_BREAKDOWN_ANALYTIC
        if (txn_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(load_log_from_logstore_end_time_);
        }

        g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T, load_log_from_logstore_end_time_ - load_log_from_logstore_begin_time_);
    #endif

        memcpy(tuple_data, (TupleData)((char*)log_entry) + 40, g_schema->GetTupleSize(table_id));
    }
    else
    {
        memcpy(tuple_data, visible_ver->tuple_data_, g_schema->GetTupleSize(table_id));
    }


#endif


    return true;
}

bool ApplyWritesOnDemandAM::ReplayTupleLog(LogTupleMeta* log_tuple_meta, 
                                           LogReplayContext* replay_context)
{
    RC rc = RC_OK;


#if    TIME_BREAKDOWN_ANALYTIC && USING_OA_REPLAY_RECORD_WRITE_INFO_TIME_T
    
    ClockTime  record_write_info_begin_time_ = 0;
    ClockTime  record_write_info_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(record_write_info_begin_time_);
    }
#endif


    AccessObj*             wi_ao            = nullptr;
    WriteInfoAO*           wi_access_object = nullptr;

    Index* unapplied_index = g_schema->GetUnappliedIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);

    rc = unapplied_index->IndexRead(log_tuple_meta->primary_key_, wi_ao);

    if (rc == RC_OK)
    {

    }
    else
    {
        wi_access_object = new WriteInfoAO();

        wi_access_object->pk_ = log_tuple_meta->primary_key_;

        
        rc = unapplied_index->IndexInsert(log_tuple_meta->primary_key_, wi_access_object);

        if (rc == RC_ERROR)
        {
  
            delete wi_access_object;

            rc = unapplied_index->IndexRead(log_tuple_meta->primary_key_, wi_ao);

            if (rc == RC_NULL)
            {
                printf("error! must exist write_info ao in unapplied_index! ApplyWritesOnDemandAM::ReplayTupleLog \n");
                exit(0);
            }

        }
        else
        {
            wi_ao = wi_access_object;
        }
    }

    LogLogicalPtr log_logical_ptr = 0;
    log_logical_ptr |= ((LogIndexTupleMeta*)log_tuple_meta)->log_buf_id_ << LOGBUF_OFFSET;
    log_logical_ptr |= ((LogIndexTupleMeta*)log_tuple_meta)->logindex_lsn_ << LOGLSN_OFFSET;

    WriteInfo* new_wi = new WriteInfo(log_tuple_meta->commit_ts_, 
                                    log_logical_ptr, 
                                    40 + g_schema->GetTupleSize(log_tuple_meta->table_id_));

    wi_access_object = dynamic_cast<WriteInfoAO*>(wi_ao);
    wi_access_object->InsertNewWriteInfo(new_wi);



#if    TIME_BREAKDOWN_ANALYTIC && USING_OA_REPLAY_RECORD_WRITE_INFO_TIME_T
    
    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(record_write_info_end_time_);

        g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                REPLAY_THREAD_T, 
                                                OA_REPLAY_RECORD_WRITE_INFO_TIME_T, 
                                                record_write_info_end_time_ - record_write_info_begin_time_);
    }

#endif


#if    MICRO_STATISTIC_ANALYTIC && USING_LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T
    if (replay_context->IsMicroStatisticSample())
    {
        g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T, 1);
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


    AccessObj*             kv_ao            = nullptr;
    ApplyWritesOnDemandAO* kv_access_object = nullptr;

    Index* primary_index   = g_schema->GetIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);

    if (log_tuple_meta->opt_type_ == INSERT_AT || log_tuple_meta->index_and_opt_cnt_ > 0)
    {

        rc = primary_index->IndexRead(log_tuple_meta->primary_key_, kv_ao);

        if (rc == RC_OK)
        {
  
        }
        else
        {
            kv_access_object = new ApplyWritesOnDemandAO();
            
            kv_access_object->pk_ = log_tuple_meta->primary_key_;

            rc = primary_index->IndexInsert(log_tuple_meta->primary_key_, kv_access_object);
    
            if (rc == RC_ERROR)
            {
                delete kv_access_object;
    
                rc = primary_index->IndexRead(log_tuple_meta->primary_key_, kv_ao);
    
                kv_access_object = dynamic_cast<ApplyWritesOnDemandAO*>(kv_ao);
        
            }
            else
            {
                kv_ao = kv_access_object;
            }
        }
        
        #if    MICRO_STATISTIC_ANALYTIC && USING_LA_REPLAY_ACCESS_KV_INDEX_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, LA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif

    
        for (size_t i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
        {
            IndexID    index_id    = log_tuple_meta->index_and_opts_[i].index_id_;
            AccessType access_type = log_tuple_meta->index_and_opts_[i].index_opt_;
    
            Index*  secondary_index = g_schema->GetIndex(index_id, log_tuple_meta->shard_id_);
    
            IndexKey     index_key = g_schema->GetIndexKey(index_id, (TupleData)log_tuple_meta->tuple_log_ptr_);
            PartitionKey part_key  = g_schema->GetPartKey(index_id, (TupleData)log_tuple_meta->tuple_log_ptr_);

            switch (access_type)
            {
            case INSERT_AT:
                secondary_index->IndexInsert(index_key, kv_ao, part_key, log_tuple_meta->commit_ts_);
                break;
            case DELETE_AT:
                secondary_index->IndexRemove(index_key, part_key, log_tuple_meta->commit_ts_);
                break;
            default:
                break;
            }

        #if    MICRO_STATISTIC_ANALYTIC && USING_LA_REPLAY_ACCESS_KV_INDEX_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, LA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif
        }
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