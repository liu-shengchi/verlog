#include "reclaim_thread.h"

#include "global.h"

#include "log_buffer.h"
#include "log_manager.h"

#include "snapshot_manager.h"
#include "snapshot.h"

#include "tuple.h"
#include "log_index_ver_chain_am.h"

#include "statistic_manager.h"

#include <string.h>


ReclaimThread::ReclaimThread(ThreadID thread_id, ThreadID reclaim_thread_id, ProcID process_id)
{
    thread_type_       = DBThreadType::RECLAIM_THREAD_T;
    thread_id_         = thread_id;
    processor_id_      = process_id;

    reclaim_thread_id_ = reclaim_thread_id;

    snapshot_ = new Snapshot();

    reclaim_log_buf_num_  = 0;
    reclaim_thread_index_ = 0;
    for (int i = 0; i < g_log_buffer_num; i++)
        reclaim_log_buf_[g_log_buffer_num] = nullptr;

    AllocLogBufToReclaimer();
}

ReclaimThread::~ReclaimThread()
{
}


void ReclaimThread::AllocLogBufToReclaimer()
{
    int alloced_thread   = 0;
    int unalloced_thread = g_reclaim_thread_num;

    while (alloced_thread < g_reclaim_thread_num)
    {
        int max_comm_div = unalloced_thread;
        for (; max_comm_div >= 1; max_comm_div--)
        {
            if (g_log_buffer_num % max_comm_div == 0)
                break;
        }
        
        if (alloced_thread + max_comm_div <= reclaim_thread_id_)
        {
            alloced_thread   += max_comm_div;
            unalloced_thread -= max_comm_div;
            
            reclaim_thread_index_++;
            continue;
        }

        reclaim_log_buf_num_ = g_log_buffer_num / max_comm_div;
        for (int i = 0; i < reclaim_log_buf_num_; i++)
        {
            reclaim_log_buf_[i] = g_log_manager->GetLogBuffer(i * max_comm_div + reclaim_thread_id_ - alloced_thread);
        }
        break;
    }
}


void ReclaimThread::Run()
{
#if THREAD_BIND_CORE
        SetAffinity();
#endif


#if    TIME_BREAKDOWN_ANALYTIC

    uint64_t time_breakdown_frequency_count = 0;
    bool     is_time_breakdown_sample       = false;

    ClockTime reclaim_write_txn_begin_time = 0;
    ClockTime reclaim_write_txn_end_time   = 0;
    
    ClockTime reclaim_idle_begin_time = 0;
    ClockTime reclaim_idle_end_time   = 0;

#endif




#if    MICRO_STATISTIC_ANALYTIC

    uint64_t micro_statistic_frequency_count = 0;
    bool     is_micro_statistic_sample       = false;

#endif





    LogBuffer* log_buf = nullptr;

    uint64_t reclaimed_total_log_size = 0;
    uint64_t make_snapshot_count      = 0;

    g_snapshot_manager->MakeSnapshot(snapshot_);

    while (g_system_state != SystemState::FINISH_STATE)
    {

        for (int log_buf_id = 0; log_buf_id < g_log_buffer_num; log_buf_id++)
        {

            #if AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
            
            if (log_buf_id % g_reclaim_thread_num == reclaim_thread_id_)
            {
                continue;
            }
            
            log_buf = g_log_manager->GetLogBuffer(log_buf_id);

            if (g_system_state != LOADING_STATE)
            {
                if (log_buf->replayed_lsn_ - log_buf->reclaimed_lsn_ < g_trigger_reclaim_log_threshold)
                    continue;   
            }
            else
            {
                if (log_buf->replayed_lsn_ - log_buf->reclaimed_lsn_ < g_trigger_reclaim_log_threshold_in_loading)
                    continue;
            }


            LogLSN    reclaim_start_lsn = log_buf->GetReclaimTxnLog(snapshot_);



        #if    TIME_BREAKDOWN_ANALYTIC
            time_breakdown_frequency_count++;

            if ((time_breakdown_frequency_count % g_time_breakdown_sample_frequency == 0) && g_system_state == TESTING_STATE)
            {
                is_time_breakdown_sample = true;
            }
            else
            {
                if (is_time_breakdown_sample == true)
                {
                    GET_CLOCK_TIME(reclaim_idle_end_time);
                    g_statistic_manager->TimeBreakdownStatc(reclaim_thread_id_, RECLAIM_THREAD_T, VL_RECLAIM_IDLE_TIME_T, reclaim_idle_end_time - reclaim_idle_begin_time);
                }
                
                is_time_breakdown_sample = false;
            }

            if (is_time_breakdown_sample)
            {
                GET_CLOCK_TIME(reclaim_write_txn_begin_time);
            }

        #endif

        #if    MICRO_STATISTIC_ANALYTIC
            micro_statistic_frequency_count++;

            if ((micro_statistic_frequency_count % g_micro_sample_frequency == 0) && g_system_state == TESTING_STATE)
            {
                is_micro_statistic_sample = true;
            }

            if (is_micro_statistic_sample)
            {
                g_statistic_manager->MicroParameterStatc(reclaim_thread_id_, RECLAIM_THREAD_T, RECLAIM_WRITE_TXN_COUNT_T, 1);
            }
        #endif


            MemPtr    log_mem_ptr       = log_buf->buffer_mem_ptr_ + (reclaim_start_lsn % g_log_buffer_size);
            uint64_t  txn_log_size      = *(uint64_t*)log_mem_ptr;
            Timestamp txn_cts           = *(Timestamp*)(log_mem_ptr + 8);

            LogLSN   reclaim_end_lsn = reclaim_start_lsn + txn_log_size;

            if (reclaim_start_lsn / g_log_buffer_size != (reclaim_end_lsn - 1) / g_log_buffer_size)
            {
                uint64_t leave_size = reclaim_end_lsn % g_log_buffer_size;
                memcpy((char*)(log_buf->buffer_mem_ptr_ + g_log_buffer_size), (char*)log_buf->buffer_mem_ptr_, leave_size);
            }
            
            /* 
            * 当前实现了基础的串行日志
            * 日志格式: 事务信息 + 元组修改信息1 + 元组修改信息2 + ... + 元组修改信息n  log format(bits)
            *   事务信息：
            *        txn_log_size(64)
            *      + commit_ts(64)
            *      
            *   元组修改信息：
            *        //元组的主键
            *      + primary_key(64)       
            *        
            *        //日志对应元组的基本信息，用于确定元组的访问信息。
            *        //shardid:  标识元组所在分区
            *        //tableid:  标识元组所属表
            *        //opt_type: 标识此次日志对应的操作类型：insert update delete
            *        //tuple_log_size: 表示该元组生成日志的大小 （单个元组日志不能超过 2^16-1 大小）
            *        //index_and_opt_size: 表示记录二级索引以及对应操作的类型所占的日志空间 (单个元组日志包含的数据修改，不能超过 2^8-1 个索引的修改)
            *      + shardid(16) + tableid(16) + opt_type(4) + tuple_log_size(16) + index_and_opt_size(12)
            *        
            *        //每个索引和操作记录，由32位/4字节组成，高4位编码索引操作类型 insert(0100) delete(1000)
            *        //低28位编码索引ID.（数据库系统不能超过 2^28-1 个索引）
            *      + index_and_opt_info_1(32) + index_and_opt_info_2(32) + ... + index_and_opt_info_n(32)
            * 
            *        //标识同元组的上一次更新日志所在的位置
            *      + prev_log_buf_id(8) + prev_log_lsn_(56)
            *        
            *        //指向同一元组前一个版本的日志的内存地址
            *      + prev_mem_ptr(64) 用于在回放阶段存放指向前一版本的指针，构造日志索引
            *      + ao_mem_ptr(64)   指向主键中索引的access_obj，主要用于加速频繁更新的元组的日志回放减少索引遍历开销。
            * 
            *        // [min_ts, max_ts) 决定了该元组版本的可见区间
            *        min_ts(64)    //创建该元组日志的事务的提交时间戳
            *      + max_ts(64)    //后一次更新的元组日志事务的提交时间戳
            *        
            *        //记录元组修改日志
            *      + tuple_log(..)
            */

            uint64_t offset = 8 + 8;
            while (offset != txn_log_size)
            {
                offset += 8;

                //access meta: shard_id, table_id, opt_type, tuple_log_size, index_and_opt_size
                uint64_t tuple_access_meta = *(uint64_t*)(log_mem_ptr + offset);
                
                uint64_t   opt_num  = (tuple_access_meta & (0x00000000f0000000)) >> 28;
                AccessType opt_type = (AccessType)0;

                while (opt_num != 1)
                {   
                    opt_num  = opt_num >> 1;
                    opt_type = (AccessType)(opt_type + 1);
                }

                uint64_t tuple_log_size = ((tuple_access_meta & (0x000000000ffff000)) >> 12);
                uint64_t index_and_opt_cnt = (tuple_access_meta & (0x0000000000000fff)) / 4;
                
                offset += 8;
                
                //index_and_opt_size
                offset += index_and_opt_cnt * 4;

                //prev_log_buf_id(8) + prev_log_lsn_(56)
                offset += 8;

                //prev_mem_ptr
                offset += 8;

                MemPtr ao_mem_ptr = *(uint64_t*)(log_mem_ptr + offset);
                //ao_mem_ptr
                offset += 8;

                //min_ts
                Timestamp min_ts = *(Timestamp*)(log_mem_ptr + offset);
                offset += sizeof(Timestamp);

                //max_ts
                Timestamp max_ts = *(Timestamp*)(log_mem_ptr + offset);
                offset += sizeof(Timestamp);

                if (!snapshot_->IsVisible(min_ts, max_ts))
                {
                #if    MICRO_STATISTIC_ANALYTIC
                    if (is_micro_statistic_sample)
                    {
                        g_statistic_manager->MicroParameterStatc(reclaim_thread_id_, RECLAIM_THREAD_T, VL_INVISIBLE_VERSION_COUNT_T, 1);
                    }
                #endif

                }
                else
                {

                #if    MICRO_STATISTIC_ANALYTIC
                    if (is_micro_statistic_sample)
                    {
                        g_statistic_manager->MicroParameterStatc(reclaim_thread_id_, RECLAIM_THREAD_T, VL_MATERIAL_VERSION_COUNT_T, 1);
                    }
                #endif

                    // Tuple* tuple_data_version = new Tuple(tuple_log_size, min_ts); 
                    Tuple* tuple_data_version = new Tuple(tuple_log_size, min_ts, max_ts); 
                    tuple_data_version->CopyTupleData((TupleData)(log_mem_ptr + offset), tuple_log_size);

                    LogIndexVerChainAO* access_obj = (LogIndexVerChainAO*)ao_mem_ptr;

                    access_obj->data_ver_chain_latch_.GetLatch();                    

                    Tuple* pre_version = nullptr;
                    Tuple* cur_version = access_obj->data_ver_chain_;


                    if (cur_version != nullptr && cur_version->max_ts_ == MAX_TIMESTAMP && min_ts >= access_obj->data_ver_chain_max_ts_)
                    {
                        cur_version->max_ts_ = access_obj->data_ver_chain_max_ts_;
                    }

                    COMPILER_BARRIER

                    while (cur_version != nullptr)
                    {
                        if (cur_version->min_ts_ < min_ts)
                        {
                            break;
                        }
                        else
                        {
                            pre_version = cur_version;
                            cur_version = cur_version->next_tuple_ver_;
                        }
                    }
                    
                    if (pre_version == nullptr)
                    {
                        tuple_data_version->next_tuple_ver_ = access_obj->data_ver_chain_;
                        access_obj->data_ver_chain_ = tuple_data_version;
                    }
                    else
                    {
                        tuple_data_version->next_tuple_ver_ = cur_version;
                        pre_version->next_tuple_ver_ = tuple_data_version;                       
                    }

                    COMPILER_BARRIER

                    Timestamp tuple_ver_max_ts = access_obj->data_ver_chain_max_ts_;

                    if (max_ts != MAX_TIMESTAMP)
                    {
                        RETRY:
                        tuple_ver_max_ts = access_obj->data_ver_chain_max_ts_;
                        if (tuple_ver_max_ts < max_ts)
                        {
                            if(!ATOM_CAS(access_obj->data_ver_chain_max_ts_, tuple_ver_max_ts, max_ts))
                            {
                                goto RETRY;
                            }
                        }                        
                    }
                    else if (tuple_ver_max_ts > min_ts)
                    {
                        tuple_data_version->max_ts_ = tuple_ver_max_ts;
                    }
                    else
                    {
                        if (!ATOM_CAS(access_obj->data_ver_chain_max_ts_, tuple_ver_max_ts, max_ts))
                        {
                            tuple_data_version->max_ts_ = access_obj->data_ver_chain_max_ts_;
                        }
                        else
                        {

                        }
                    }

                    access_obj->data_ver_chain_latch_.ReleaseLatch();
                }
                
                offset += tuple_log_size;
            }

            COMPILER_BARRIER

            log_buf->next_reclaimed_lsn_ = reclaim_start_lsn + txn_log_size;


            reclaimed_total_log_size += txn_log_size;
            if (reclaimed_total_log_size / g_make_snapshot_interval != make_snapshot_count)
            {
                g_snapshot_manager->MakeSnapshot(snapshot_);
                make_snapshot_count = reclaimed_total_log_size / g_make_snapshot_interval;
            }



        #if    TIME_BREAKDOWN_ANALYTIC
            if (is_time_breakdown_sample)
            {
                GET_CLOCK_TIME(reclaim_write_txn_end_time);

                g_statistic_manager->TimeBreakdownStatc(reclaim_thread_id_, RECLAIM_THREAD_T, VL_RECLAIM_WRITE_TXN_TIME_T, reclaim_write_txn_end_time - reclaim_write_txn_begin_time);

                reclaim_idle_begin_time = reclaim_write_txn_end_time;
            }
        #endif


        #if    MICRO_STATISTIC_ANALYTIC
            if (is_micro_statistic_sample)
            {
                is_micro_statistic_sample = false;
            }
        #endif


         #elif AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM

         #elif AM_STRATEGY_TYPE == QUERY_FRESH_AM

         #endif
        }
    }
}

