#include "queryfresh_am.h"

#include "schema.h"

#include "index.h"
#include "array_index.h"
#include "tuple.h"

#include "log_strategy.h"
#include "logindex_log.h"

#include "log_replay_context.h"

#include "txn_context.h"
#include "txn_id.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "statistic_manager.h"

#include "db_spin_lock.h"

#include <string.h>


#if  AM_STRATEGY_TYPE == QUERY_FRESH_AM

QueryFreshAO::QueryFreshAO()
{
    data_ver_chain_   = nullptr;
    log_logical_addr_ = 0;
    deleted_          = false;
}


QueryFreshAM::QueryFreshAM()
{

}

QueryFreshAM::~QueryFreshAM()
{

}


bool QueryFreshAM::GetTupleData(AccessObj* access_obj, 
                                TableID table_id, 
                                TupleData tuple_data, 
                                TxnContext* txn_context,
                                ShardID    shard_id,
                                PrimaryKey primary_key)
{
    QueryFreshAO* queryfresh_ao = dynamic_cast<QueryFreshAO*>(access_obj);


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


    ReplayArrayAO* replay_array_ao = nullptr;
    Index* replay_array = g_schema->GetReplayArray(table_id, shard_id);
    replay_array->IndexRead(primary_key, access_obj);
    replay_array_ao = dynamic_cast<ReplayArrayAO*>(access_obj);

#if    TIME_BREAKDOWN_ANALYTIC
    if (txn_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(fetch_unapplied_write_info_end_time_);

        g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T, fetch_unapplied_write_info_end_time_ - fetch_unapplied_write_info_begin_time_);
    }
#endif


#if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T
    if (txn_context->IsMicroStatisticSample())
    {
        g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T, 1);
    }
#endif


    
    Timestamp visible_ts = txn_context->GetReadTS();

    uint64_t  tuple_size  = g_schema->GetTupleSize(table_id);


#if    TIME_BREAKDOWN_ANALYTIC
    if (txn_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(create_version_begin_time_);
    }
#endif


#if   QUERY_FRESH_VERSION_CHAIN_RETRY

retry:

    if (queryfresh_ao->data_ver_chain_ != nullptr
         &&  queryfresh_ao->data_ver_chain_->visible_ts_ >= visible_ts)
    {

    }
    else
    {
        LogLogicalPtr start_log_ptr = replay_array_ao->log_logical_addr_;
        LogLogicalPtr end_log_ptr   = queryfresh_ao->log_logical_addr_;
        
        if (start_log_ptr == end_log_ptr)
        {

        }
        else
        {
            Tuple* newest_version = nullptr;
            Tuple* oldest_version = nullptr;
            char   tuple_log_buf[MAX_TUPLE_LOG_SIZE] = {'0'};

            LogLogicalPtr scan_log_ptr  = start_log_ptr;

            int round = 0;
            while (scan_log_ptr != end_log_ptr)
            {

            #if    TIME_BREAKDOWN_ANALYTIC
                if (txn_context->IsTimeBreakdownSample())
                {
                    GET_CLOCK_TIME(load_log_from_logstore_begin_time_);
                }
            #endif

                LogBufID log_buf_id    = (scan_log_ptr & 0xff00000000000000) >> 56;
                LogLSN   log_start_lsn = (scan_log_ptr & 0x00ffffffffffffff);
                LogLSN   log_end_lsn   = log_start_lsn + 40 + tuple_size;    //40是meta属性大小
                uint64_t tuple_log_size = log_end_lsn - log_start_lsn;

                LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);
                
                bool in_log_buf = true;

                while (!(log_buf->replayed_lsn_ >= log_end_lsn))
                {
                    PAUSE
                }

                if (log_buf->free_lsn_ < log_start_lsn)
                {
                    COMPILER_BARRIER
                    memcpy(tuple_log_buf, log_buf->buffer_ + (log_start_lsn % log_buf->buffer_size_), tuple_log_size);
                    COMPILER_BARRIER

                    if (!(log_buf->free_lsn_ < log_start_lsn))
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
                    log_buf->ReadFromDurable(tuple_log_buf, tuple_log_size, log_start_lsn);

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

            #if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_LOAD_LOG_COUNT_T
                if (txn_context->IsMicroStatisticSample())
                {
                    g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_LOAD_LOG_COUNT_T, 1);
                }
            #endif


                scan_log_ptr        = *(LogLogicalPtr*)((char*)tuple_log_buf);
                Timestamp commit_ts = *(Timestamp*)(((char*)tuple_log_buf) + 24);  //min_ts

                Tuple* new_version = new Tuple(tuple_size, commit_ts);
                new_version->CopyTupleData((TupleData)((char*)tuple_log_buf) + 40, tuple_size);
                
            #if     MICRO_STATISTIC_ANALYTIC && USING_LA_EXECUTE_CREATE_VERSION_COUNT_T
                if (txn_context->IsMicroStatisticSample())
                {
                    g_statistic_manager->MicroParameterStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, LA_EXECUTE_CREATE_VERSION_COUNT_T, 1);
                }
            #endif

                COMPILER_BARRIER

                if (newest_version == nullptr)
                {
                    newest_version = new_version;
                    oldest_version = new_version;
                }
                else
                {
                    oldest_version->next_tuple_ver_ = new_version;
                    oldest_version = new_version;
                }
                round++;
            }

            if (newest_version == nullptr)
            {
                printf("error! newest_version不应为nullptr\n");
            }
            
            queryfresh_ao->spin_lock_.GetSpinLock();

            if (end_log_ptr != queryfresh_ao->log_logical_addr_)
            {
                while (newest_version != nullptr)
                {
                    oldest_version = newest_version->next_tuple_ver_;
                    delete newest_version;
                    newest_version = oldest_version;
                }
                
                queryfresh_ao->spin_lock_.ReleaseSpinLock();

                goto retry;
            }
            
            oldest_version->next_tuple_ver_ = queryfresh_ao->data_ver_chain_;
            queryfresh_ao->data_ver_chain_ = newest_version;
            queryfresh_ao->log_logical_addr_ = start_log_ptr;
            
            queryfresh_ao->spin_lock_.ReleaseSpinLock();

        }
    }
#else

    if (queryfresh_ao->data_ver_chain_ != nullptr
         &&  queryfresh_ao->data_ver_chain_->visible_ts_ >= visible_ts)
    {

    }
    else
    {
        queryfresh_ao->spin_lock_.GetSpinLock();

        LogLogicalPtr start_log_ptr = replay_array_ao->log_logical_addr_;
        LogLogicalPtr end_log_ptr   = queryfresh_ao->log_logical_addr_;
        
        if (start_log_ptr == end_log_ptr)
        {

        }
        else
        {
            Tuple* newest_version = nullptr;
            Tuple* oldest_version = nullptr;
            char   tuple_log_buf[MAX_TUPLE_LOG_SIZE] = {'0'};

            LogLogicalPtr scan_log_ptr  = start_log_ptr;

            int round = 0;
            while (scan_log_ptr != end_log_ptr)
            {
                LogBufID log_buf_id    = (scan_log_ptr & 0xff00000000000000) >> 56;
                LogLSN   log_start_lsn = (scan_log_ptr & 0x00ffffffffffffff);
                LogLSN   log_end_lsn   = log_start_lsn + 40 + tuple_size;    //40是meta属性大小
                uint64_t tuple_log_size = log_end_lsn - log_start_lsn;

                LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);
                
                bool in_log_buf = true;

                while (!(log_buf->replayed_lsn_ >= log_end_lsn))
                {
                    PAUSE
                }

                if (log_buf->free_lsn_ < log_start_lsn)
                {
                    COMPILER_BARRIER
                    memcpy(tuple_log_buf, log_buf->buffer_ + (log_start_lsn % log_buf->buffer_size_), tuple_log_size);
                    COMPILER_BARRIER

                    if (!(log_buf->free_lsn_ < log_start_lsn))
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
                    log_buf->ReadFromDurable(tuple_log_buf, tuple_log_size, log_start_lsn);
                }
                
                scan_log_ptr        = *(LogLogicalPtr*)((char*)tuple_log_buf);
                Timestamp commit_ts = *(Timestamp*)(((char*)tuple_log_buf) + 24);  //min_ts

                Tuple* new_version = new Tuple(tuple_size, commit_ts);
                new_version->CopyTupleData((TupleData)((char*)tuple_log_buf) + 40, tuple_size);
                
                COMPILER_BARRIER

                if (newest_version == nullptr)
                {
                    newest_version = new_version;
                    oldest_version = new_version;
                }
                else
                {
                    oldest_version->next_tuple_ver_ = new_version;
                    oldest_version = new_version;
                }
                round++;
            }

            if (newest_version == nullptr)
            {
                printf("error! newest_version不应为nullptr\n");
            }
            
            oldest_version->next_tuple_ver_ = queryfresh_ao->data_ver_chain_;
            queryfresh_ao->data_ver_chain_ = newest_version;
            queryfresh_ao->log_logical_addr_ = start_log_ptr;
        }

        queryfresh_ao->spin_lock_.ReleaseSpinLock();
    }
#endif



#if    TIME_BREAKDOWN_ANALYTIC
    if (txn_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(create_version_end_time_);
        
        g_statistic_manager->TimeBreakdownStatc(txn_context->GetExecuteThreadID(), TXN_THREAD_T, OA_EXECUTE_CREATE_VERSION_TIME_T, create_version_end_time_ - create_version_begin_time_);
    }
#endif

    
    Tuple* tuple = queryfresh_ao->data_ver_chain_;
    
    bool find = true;
    while (true)
    {
        if (tuple->visible_ts_ <= visible_ts)
        {
            memcpy(tuple_data, (void*)tuple->tuple_data_, g_schema->GetTupleSize(table_id));
            find = true;
            break;
        }
        
        tuple = tuple->next_tuple_ver_;
        if (tuple == nullptr)
        {
            find = false;
            break;
        }
    }

    return find;
}


bool QueryFreshAM::ReplayTupleLog(LogTupleMeta*     log_tuple_meta, 
                                  LogReplayContext* replay_context)
{
    RC rc = RC::RC_OK;
 
#if    TIME_BREAKDOWN_ANALYTIC && USING_OA_REPLAY_RECORD_WRITE_INFO_TIME_T
    
    ClockTime  record_write_info_begin_time_ = 0;
    ClockTime  record_write_info_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(record_write_info_begin_time_);
    }
#endif

    LogIndexTupleMeta* logindex_tuple_meta = dynamic_cast<LogIndexTupleMeta*>(log_tuple_meta);

    AccessObj*       replay_obj      = nullptr;
    AccessObj*       queryfresh_obj  = nullptr;

    ReplayArrayAO*   replay_array_ao = nullptr;
    QueryFreshAO*    queryfresh_ao   = nullptr;

    Index*      primary_index = g_schema->GetIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);

#if  QUERY_FRESH_REPLAY_ARRAY
    ArrayIndex* replay_array  = (ArrayIndex*)g_schema->GetReplayArray(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);

    replay_array->IndexRead(log_tuple_meta->primary_key_, replay_obj);
    replay_array_ao = dynamic_cast<ReplayArrayAO*>(replay_obj);
#else
    Index* replay_index  = g_schema->GetReplayArray(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);

    rc = replay_index->IndexRead(log_tuple_meta->primary_key_, replay_obj);

    if (rc == RC_OK)
    {
        replay_array_ao = dynamic_cast<ReplayArrayAO*>(replay_obj);
        
    }
    else
    {
        replay_array_ao = new ReplayArrayAO();
        
        rc = replay_index->IndexInsert(log_tuple_meta->primary_key_, replay_array_ao);

        if (rc == RC_ERROR)
        {

            delete replay_array_ao;

            replay_index->IndexRead(log_tuple_meta->primary_key_, replay_obj);

            replay_array_ao = dynamic_cast<ReplayArrayAO*>(replay_obj);
        }
        else
        {
            replay_obj = replay_array_ao;
        }
    }
#endif

    bool need_create_index_entry = false;
    if(replay_array_ao->log_logical_addr_ == 0)
    {
        need_create_index_entry = true;
    }

    
    replay_array_ao->spin_lock_.GetSpinLock();

    if (log_tuple_meta->commit_ts_ > replay_array_ao->commit_ts_ || log_tuple_meta->commit_ts_ == 0)
    {
        LogLogicalPtr log_logical_ptr = 0;
        log_logical_ptr |= logindex_tuple_meta->log_buf_id_ << LOGBUF_OFFSET;
        log_logical_ptr |= logindex_tuple_meta->logindex_lsn_ << LOGLSN_OFFSET;

        replay_array_ao->log_logical_addr_ = log_logical_ptr;
        replay_array_ao->commit_ts_        = log_tuple_meta->commit_ts_;
    }

    replay_array_ao->spin_lock_.ReleaseSpinLock();




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
    
    ClockTime  access_index_begin_time_ = 0;
    ClockTime  access_index_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(access_index_begin_time_);
    }
#endif


    if (need_create_index_entry)
    {
        queryfresh_ao = new QueryFreshAO();

        queryfresh_ao->oid_ = log_tuple_meta->primary_key_;

        rc = primary_index->IndexInsert(log_tuple_meta->primary_key_, queryfresh_ao);

        if (rc == RC_ERROR)
        {
            delete queryfresh_ao;
            queryfresh_ao = nullptr;
        }
        else
        {
        }

    #if    MICRO_STATISTIC_ANALYTIC && USING_LA_REPLAY_ACCESS_KV_INDEX_COUNT_T
        if (replay_context->IsMicroStatisticSample())
        {
            g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, LA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
        }
    #endif

    }






    if (log_tuple_meta->index_and_opt_cnt_ != 0)
    {
        if (queryfresh_ao == nullptr)
        {
            primary_index->IndexRead(log_tuple_meta->primary_key_, queryfresh_obj);
            queryfresh_ao = dynamic_cast<QueryFreshAO*>(queryfresh_obj);

        #if    MICRO_STATISTIC_ANALYTIC && USING_LA_REPLAY_ACCESS_KV_INDEX_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, LA_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif
        }
        else
        {

        }
    }
    


    for (size_t i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
    {
        IndexID    index_id    = log_tuple_meta->index_and_opts_[i].index_id_;
        AccessType access_type = log_tuple_meta->index_and_opts_[i].index_opt_;

        Index*  secondary_index = g_schema->GetIndex(index_id, log_tuple_meta->shard_id_);

        IndexKey     index_key = g_schema->GetIndexKey(index_id, (TupleData)(logindex_tuple_meta->logindex_ptr_ + 40));
        PartitionKey part_key  = g_schema->GetPartKey(index_id, (TupleData)(logindex_tuple_meta->logindex_ptr_ + 40));

        switch (access_type)
        {
        case INSERT_AT:
            secondary_index->IndexInsert(index_key, queryfresh_ao, part_key, log_tuple_meta->commit_ts_);
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


#if    TIME_BREAKDOWN_ANALYTIC && USING_EALA_REPLAY_ACCESS_KV_INDEX_TIME_T
    
    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(access_index_end_time_);

        g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                REPLAY_THREAD_T, 
                                                EALA_REPLAY_ACCESS_KV_INDEX_TIME_T, 
                                                access_index_end_time_ - access_index_begin_time_);
    }

#endif


    return true;
}


#endif