#include "log_index_ver_chain_am.h"

#include "global.h"

#include "schema.h"
#include "tpcc_schema.h"

#include "index.h"
#include "tuple.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "log_strategy.h"
#include "log_replay_context.h"
#include "logindex_log.h"

#include "txn_context.h"
#include "txn_id.h"

#include "statistic_manager.h"

#include <string.h>



#if  AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM

LogIndexVerChainAM::LogIndexVerChainAM()
{
}

LogIndexVerChainAM::~LogIndexVerChainAM()
{
}


bool LogIndexVerChainAM::GetTupleData(AccessObj* access_obj, 
                                      TableID table_id, TupleData tuple_data, Timestamp visible_ts, ThreadID txn_thread_id)
{
    LogIndexVerChainAO* logindex_ver_chain_ao = dynamic_cast<LogIndexVerChainAO*>(access_obj);


Find:
    return true;
Unfind:
    return false;

}


bool LogIndexVerChainAM::GetTupleData(AccessObj* access_obj, 
                                      TableID table_id, 
                                      TupleData tuple_data, 
                                      TxnContext* txn_context,
                                      ShardID    shard_id,
                                      PrimaryKey primary_key)
{
    LogIndexVerChainAO* logindex_ver_chain_ao = dynamic_cast<LogIndexVerChainAO*>(access_obj);

    Timestamp visible_ts = txn_context->GetReadTS();

    LogBuffer*    log_buf         = nullptr;
    LogLogicalPtr log_logical_ptr = logindex_ver_chain_ao->log_logical_ptr_;
    MemPtr        log_mem_ptr     = logindex_ver_chain_ao->log_mem_ptr_;

    Timestamp log_ver_min_ts = 0;
    LogBufID  cur_log_buf_id = 0;
    LogLSN    cur_log_lsn    = 0;

    bool      scan_log_buf   = false;
    bool      reclaimed      = false;


    int access_type = 0;

    if (logindex_ver_chain_ao->data_ver_chain_max_ts_ == MAX_TIMESTAMP
        || logindex_ver_chain_ao->data_ver_chain_max_ts_ <= visible_ts)
    {

        scan_log_buf = true;

    #if    OPTIMISTIC_READ_IN_INDEXLOG

        while (true)
        {
            cur_log_buf_id = (log_logical_ptr & LOGBUF_MASK) >> LOGBUF_OFFSET;
            cur_log_lsn    = (log_logical_ptr & LOGLSN_MASK) >> LOGLSN_OFFSET;

            log_buf = g_log_manager->GetLogBuffer(cur_log_buf_id);

            if (cur_log_lsn < log_buf->next_reclaimed_lsn_)
            {
                reclaimed = true;

                access_type = 1;

                break;
            }

            log_ver_min_ts = *(Timestamp*)(log_mem_ptr + 24);

            if (cur_log_lsn < log_buf->next_reclaimed_lsn_)
            {
                reclaimed = true;

                access_type = 1;
                
                break;
            }

            if (log_ver_min_ts <= visible_ts)
            {
                memcpy(tuple_data, (void*)(log_mem_ptr + 40), g_schema->GetTupleSize(table_id));
                
                if (cur_log_lsn < log_buf->next_reclaimed_lsn_)
                {
                    reclaimed = true;
    
                    access_type = 1;
                    
                    break;
                }
                else
                {
                    access_type = 2;
                    break;
                }
            }


            log_logical_ptr = *(uint64_t*)(log_mem_ptr);
            log_mem_ptr     = *(MemPtr*)(log_mem_ptr + 8);

            if (cur_log_lsn < log_buf->next_reclaimed_lsn_)
            {
                reclaimed = true;

                access_type = 1;
                
                break;
            }

            if (log_logical_ptr == 0)
            {
                access_type = 3;
                break;
            }
        }


    #else

        while (true)
        {
            cur_log_buf_id = (log_logical_ptr & LOGBUF_MASK) >> LOGBUF_OFFSET;
            cur_log_lsn    = (log_logical_ptr & LOGLSN_MASK) >> LOGLSN_OFFSET;

            log_buf = g_log_manager->GetLogBuffer(cur_log_buf_id);

            txn_context->accessing_lsn_[cur_log_buf_id] = cur_log_lsn;
            COMPILER_BARRIER
            txn_context->accessing_logbuf_[cur_log_buf_id] = true;
            COMPILER_BARRIER

            if (cur_log_lsn < log_buf->reclaimed_lsn_)
            {
                reclaimed = true;

                access_type = 1;
                COMPILER_BARRIER
                txn_context->accessing_logbuf_[cur_log_buf_id] = false;
                break;
            }

            log_ver_min_ts = *(Timestamp*)(log_mem_ptr + 24);
            if (log_ver_min_ts <= visible_ts)
            {
                memcpy(tuple_data, (void*)(log_mem_ptr + 40), g_schema->GetTupleSize(table_id));
                COMPILER_BARRIER
                txn_context->accessing_logbuf_[cur_log_buf_id] = false;
                access_type = 2;
                break;
            }

            log_logical_ptr = *(uint64_t*)(log_mem_ptr);
            log_mem_ptr     = *(MemPtr*)(log_mem_ptr + 8);

            if (log_logical_ptr == 0)
            {
                access_type = 3;
                COMPILER_BARRIER
                txn_context->accessing_logbuf_[cur_log_buf_id] = false;
                break;
            }
        }

    #endif

    }
    

    if (access_type == 0 || access_type == 1)
    {
        RETRY_SCAN_TUPLE_VER_CHAIN:

        Tuple* tuple = logindex_ver_chain_ao->data_ver_chain_;

        while (true && g_system_state != SystemState::FINISH_STATE)
        {
            if (tuple == nullptr)
            {
                access_type = 5;
                break;
            }
            else if (tuple->max_ts_ <= visible_ts)
            {
                if (logindex_ver_chain_ao->created_ts_ > visible_ts)
                {
                    access_type = 5;
                    break;
                }

                goto RETRY_SCAN_TUPLE_VER_CHAIN;
            }

            if (tuple->min_ts_ <= visible_ts && tuple->max_ts_ > visible_ts)
            {
                memcpy(tuple_data, (void*)tuple->tuple_data_, g_schema->GetTupleSize(table_id));
                access_type = 4;
                break;
            }
            
            tuple = tuple->next_tuple_ver_;
        }
    }
    
    switch (access_type)
    {
    case 2:

    #if    MICRO_STATISTICAL_DATA_ANALYTIC
        if (txn_context->txn_id_->thread_id_ == (g_txn_thread_num - 1))
        {
            count9++;
        }
    #endif

        goto Find;
        break;
    case 4:

    #if    MICRO_STATISTICAL_DATA_ANALYTIC
        if (txn_context->txn_id_->thread_id_ == (g_txn_thread_num - 1))
        {
            count10++;
        }
    #endif

        goto Find;
        break;
    case 3:
    case 5:
        goto Unfind;
        break;
    }
    

Find:
    return true;
Unfind:
    return false;

}



bool LogIndexVerChainAM::ReplayTupleLog(LogTupleMeta* log_tuple_meta, LogReplayContext* replay_context)
{

#if    TIME_BREAKDOWN_ANALYTIC && USING_VL_MAINTAIN_LOG_CHAIN_TIME_T
    
    ClockTime  vl_maintain_log_chian_begin_time_ = 0;
    ClockTime  vl_maintain_log_chian_end_time_   = 0;

    if (replay_context->IsTimeBreakdownSample())
    {
        GET_CLOCK_TIME(vl_maintain_log_chian_begin_time_);
    }
#endif


    LogIndexTupleMeta* logindex_tuple_meta = dynamic_cast<LogIndexTupleMeta*>(log_tuple_meta);

    LogBufID   prev_log_buf_id  = 0;
    LogLSN     prev_log_lsn     = 0;
    MemPtr     prev_log_mem_ptr = 0x0;
    LogBuffer* prev_log_buf     = nullptr;
    MemPtr     ao_mem_ptr       = 0x0;


    ThreadID replay_thread_id = replay_context->GetReplayThreadID();

    uint64_t replay_type = 0;

    switch (logindex_tuple_meta->opt_type_)
    {
    case UPDATE_AT:
    case DELETE_AT:
        {
            prev_log_buf_id = logindex_tuple_meta->prev_log_buf_id_;
            prev_log_lsn    = logindex_tuple_meta->prev_log_lsn_;
            prev_log_mem_ptr = logindex_tuple_meta->prev_log_mem_ptr_;
            prev_log_buf   = g_log_manager->GetLogBuffer(logindex_tuple_meta->prev_log_buf_id_);

            memcpy((void*)(logindex_tuple_meta->logindex_ptr_ + 8), &prev_log_mem_ptr, sizeof(MemPtr));

            // COMPILER_BARRIER
            prev_log_buf->modifying_lsn_[replay_thread_id] = prev_log_lsn + 40;
            COMPILER_BARRIER
            prev_log_buf->modifying_[replay_thread_id] = true;
            COMPILER_BARRIER

            if (prev_log_lsn + 40 < prev_log_buf->protected_reclaim_lsn_)
            {
                prev_log_buf->modifying_[replay_thread_id] = false;
                replay_type = 1;
            }
            else if (prev_log_lsn + 40 > prev_log_buf->lsn_)
            {
                prev_log_buf->modifying_[replay_thread_id] = false;
                replay_type = 2;
            }
            else
            {
                uint64_t max_ts = *(Timestamp*)(prev_log_mem_ptr + 32);
                if (max_ts != 0xffffffffffffffff)
                {
                    replay_type = 3;
                }
                else
                {
                    COMPILER_BARRIER
                    ao_mem_ptr = *(MemPtr*)(prev_log_mem_ptr + 16);
                    memcpy((void*)(prev_log_mem_ptr + 32), &logindex_tuple_meta->commit_ts_, sizeof(Timestamp));
                    replay_type = 4;
                }

                COMPILER_BARRIER
                prev_log_buf->modifying_[replay_thread_id] = false;
            }
        }
        break;

    case INSERT_AT:
        replay_type = 5;
        break;

    default:
        break;
    }



    AccessObj*          access_obj = nullptr;
    LogIndexVerChainAO* tuple_ao   = nullptr;
 
    LogLogicalPtr log_logical_ptr = 0;
    log_logical_ptr |= logindex_tuple_meta->log_buf_id_ << LOGBUF_OFFSET;
    log_logical_ptr |= logindex_tuple_meta->logindex_lsn_ << LOGLSN_OFFSET;

    bool replay_success = false;

    switch (replay_type)
    {
    case 1:
        {
            Index* primary_index = g_schema->GetIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);
            if (primary_index == nullptr)
            {
                printf("error! primary index == nullptr!\n");
                exit(0);
            }

            RC rc = primary_index->IndexRead(log_tuple_meta->primary_key_, access_obj);
            if (rc != RC_OK)
            {
                printf("错误, 未找到access_obj, 前序版本本应已创建access_obj!\n");
                exit(0);
            }
            tuple_ao = dynamic_cast<LogIndexVerChainAO*>(access_obj);


        #if    MICRO_STATISTIC_ANALYTIC && USING_VL_REPLAY_CREATE_VERSION_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, VL_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif


            tuple_ao->data_ver_chain_max_ts_ = logindex_tuple_meta->commit_ts_;

            COMPILER_BARRIER

            tuple_ao->log_mem_ptr_ = logindex_tuple_meta->logindex_ptr_;
            tuple_ao->log_logical_ptr_ = log_logical_ptr;

            COMPILER_BARRIER
            memcpy((void*)(logindex_tuple_meta->logindex_ptr_ + 16), &tuple_ao, sizeof(MemPtr));
            COMPILER_BARRIER
            memset((void*)(logindex_tuple_meta->logindex_ptr_ + 32), 0xff, sizeof(Timestamp));
            
            replay_success = true;
        }
        break;

    case 2:
    case 3:

        replay_success = false;

        break;

    case 4:
        {
            tuple_ao = (LogIndexVerChainAO*)ao_mem_ptr;
            
            COMPILER_BARRIER

            tuple_ao->log_mem_ptr_     = logindex_tuple_meta->logindex_ptr_;
            tuple_ao->log_logical_ptr_ = log_logical_ptr;

            COMPILER_BARRIER
            memcpy((void*)(logindex_tuple_meta->logindex_ptr_ + 16), &tuple_ao, sizeof(MemPtr));

            COMPILER_BARRIER
            memset((void*)(logindex_tuple_meta->logindex_ptr_ + 32), 0xff, sizeof(Timestamp));

            replay_success = true;

        #if    MICRO_STATISTIC_ANALYTIC && USING_VL_FAST_REPLAY_PATH_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, VL_FAST_REPLAY_PATH_COUNT_T, 1);
            }
        #endif

        }
        break;

    case 5:
        {
            tuple_ao = new LogIndexVerChainAO();
            tuple_ao->log_mem_ptr_     = logindex_tuple_meta->logindex_ptr_;
            tuple_ao->log_logical_ptr_ = log_logical_ptr;
            tuple_ao->created_ts_      = logindex_tuple_meta->commit_ts_;

            Index* primary_index = g_schema->GetIndex(log_tuple_meta->table_id_, log_tuple_meta->shard_id_);
            RC rc = primary_index->IndexInsert(log_tuple_meta->primary_key_, tuple_ao);
            if (rc == RC_ERROR)
            {
                printf("错误, 主键索引中已存在该元组, 不应该!\n");
                exit(0);
            }

        #if    MICRO_STATISTIC_ANALYTIC && USING_VL_REPLAY_CREATE_VERSION_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, VL_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif

            memcpy((void*)(logindex_tuple_meta->logindex_ptr_ + 16), &tuple_ao, sizeof(MemPtr));
            COMPILER_BARRIER
            memset((void*)(logindex_tuple_meta->logindex_ptr_ + 32), 0xff, sizeof(Timestamp));

            replay_success = true;
        
        #if    MICRO_STATISTICAL_DATA_ANALYTIC
            if (replay_thread_id == (g_replay_thread_num - 1) && g_system_state != LOADING_STATE)
            {
                count6++;
            }
        #endif
        
        }
        break;
    }


    if (replay_success)
    {
        access_obj = tuple_ao;
        
        for (size_t i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
        {
            IndexID    index_id    = log_tuple_meta->index_and_opts_[i].index_id_;
            AccessType access_type = log_tuple_meta->index_and_opts_[i].index_opt_;

            Index*  secondary_index = g_schema->GetIndex(index_id, log_tuple_meta->shard_id_);

            IndexKey     index_key = g_schema->GetIndexKey(index_id, (TupleData)logindex_tuple_meta->tuple_log_ptr_);
            PartitionKey part_key  = g_schema->GetPartKey(index_id, (TupleData)logindex_tuple_meta->tuple_log_ptr_);

            switch (access_type)
            {
            case INSERT_AT:
                secondary_index->IndexInsert(index_key, access_obj, part_key, logindex_tuple_meta->commit_ts_);
                break;
            case DELETE_AT:
                secondary_index->IndexRemove(index_key, part_key, logindex_tuple_meta->commit_ts_);
                break;
            default:
                break;
            }

        #if    MICRO_STATISTIC_ANALYTIC && USING_VL_REPLAY_CREATE_VERSION_COUNT_T
            if (replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_context->GetReplayThreadID(), REPLAY_THREAD_T, VL_REPLAY_ACCESS_KV_INDEX_COUNT_T, 1);
            }
        #endif

        }


    #if    TIME_BREAKDOWN_ANALYTIC && USING_VL_MAINTAIN_LOG_CHAIN_TIME_T
        
        if (replay_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(vl_maintain_log_chian_end_time_);

            g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                    REPLAY_THREAD_T, 
                                                    VL_MAINTAIN_LOG_CHAIN_TIME_T, 
                                                    vl_maintain_log_chian_end_time_ - vl_maintain_log_chian_begin_time_);
        }

    #endif


        return true;
    }
    else
    {
    #if    TIME_BREAKDOWN_ANALYTIC && USING_VL_MAINTAIN_LOG_CHAIN_TIME_T
        
        if (replay_context->IsTimeBreakdownSample())
        {
            GET_CLOCK_TIME(vl_maintain_log_chian_end_time_);

            g_statistic_manager->TimeBreakdownStatc(replay_context->GetReplayThreadID(), 
                                                    REPLAY_THREAD_T, 
                                                    VL_MAINTAIN_LOG_CHAIN_TIME_T, 
                                                    vl_maintain_log_chian_end_time_ - vl_maintain_log_chian_begin_time_);
        }

    #endif
    
        return false;
    }
    
}

#endif