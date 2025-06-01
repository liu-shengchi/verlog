#include "logindex_log.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "log_replay_context.h"

#include "am_strategy.h"

#include "statistic_manager.h"

#include "string.h"


#if   LOG_STRATEGY_TYPE == LOGINDEX_LOG

/***********************************************/
/***************** LogIndexLog *****************/
/***********************************************/

LogIndexLog::LogIndexLog()
{
}

LogIndexLog::~LogIndexLog()
{
}


void LogIndexLog::DeconstructTxnLog(LogReplayContext* log_replay_context)
{
    LogBuffer*         replay_log_buf = log_replay_context->replay_log_buffer_;
    LogIndexTxnMeta*   log_txn_meta   = dynamic_cast<LogIndexTxnMeta*>(log_replay_context->log_txn_meta_);
    LogIndexTupleMeta* log_tuple_meta = nullptr;

    uint64_t txn_log_size  = log_txn_meta->log_size_;
    LogLSN   start_lsn     = log_txn_meta->start_lsn_;

    /**
     * 后续需要对日志进行解析操作，
     * 因此需要确定日志所在的连续空间的起始地址。
     */
    char* txn_log_ = (char*)log_txn_meta->log_start_ptr_;

    log_txn_meta->log_buf_id_ = log_replay_context->replay_log_buf_id_;

    uint64_t deconstruct_offset = 0;

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

    /* 解析事务日志的元数据 */
    //txn_log_size
    uint64_t test = *(uint64_t*)txn_log_;
    if (test == 0)
    {
        printf("error!\n");
    }
    deconstruct_offset += 8;

    //commit_ts
    log_txn_meta->commit_ts_ = *(Timestamp*)(txn_log_ + deconstruct_offset);
    deconstruct_offset += sizeof(Timestamp);

    /* 
     * 在日志缓冲区中，记录该日志回放上下文正在回放的事务日志的CommitTS。
     */
    if (log_replay_context->unreplayed_log_tuple_.empty())
    {
        replay_log_buf->replaying_commit_ts_[log_replay_context->replay_context_index_] = log_txn_meta->commit_ts_;
    }

    /**
     * 解析日志记录的元组
     * **/
    log_replay_context->log_tuple_num_ = 0;
    while (deconstruct_offset < txn_log_size)
    {
        log_tuple_meta = (LogIndexTupleMeta*)log_replay_context->log_tuple_meta_[log_replay_context->log_tuple_num_];

        log_tuple_meta->commit_ts_ = log_txn_meta->commit_ts_;

        log_tuple_meta->log_buf_id_ = log_txn_meta->log_buf_id_;
        log_tuple_meta->start_lsn_  = start_lsn + deconstruct_offset;

        log_tuple_meta->log_logical_ptr_ = (((uint64_t)log_tuple_meta->log_buf_id_ << LOGBUF_OFFSET) & LOGBUF_MASK) | (((uint64_t)log_tuple_meta->start_lsn_ << LOGLSN_OFFSET) & LOGLSN_MASK);

        //primary_key
        log_tuple_meta->primary_key_ = *(PrimaryKey*)(txn_log_ + deconstruct_offset);
        deconstruct_offset += sizeof(PrimaryKey);

        //access meta: shard_id, table_id, opt_type, tuple_log_size, index_and_opt_size
        uint64_t tuple_access_meta = 0;
        tuple_access_meta = *(uint64_t*)(txn_log_ + deconstruct_offset);
        deconstruct_offset += sizeof(tuple_access_meta);

        log_tuple_meta->shard_id_   = (tuple_access_meta & (0xffff000000000000)) >> 48;
        log_tuple_meta->table_id_   = (tuple_access_meta & (0x0000ffff00000000)) >> 32;
        
        uint64_t opt_num = (AccessType)((tuple_access_meta & (0x00000000f0000000)) >> 28);
        log_tuple_meta->opt_type_ = (AccessType)0;

        while (opt_num != 1)
        {   
            opt_num = opt_num >> 1;
            log_tuple_meta->opt_type_ = (AccessType)(log_tuple_meta->opt_type_ + 1);
        }

        log_tuple_meta->tuple_log_size_ = ((tuple_access_meta & (0x000000000ffff000)) >> 12);

        log_tuple_meta->index_and_opt_cnt_ = (tuple_access_meta & (0x0000000000000fff)) / 4;

        if (log_tuple_meta->index_and_opt_cnt_ != 0)
        {
            printf("");
        }
        

        for (int i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
        {
            uint32_t index_opt_log = 0;
            index_opt_log = *(uint32_t*)(txn_log_ + deconstruct_offset);
            log_tuple_meta->index_and_opts_[i].index_opt_ = (AccessType)((index_opt_log & 0xf0000000) >> 28);
            log_tuple_meta->index_and_opts_[i].index_id_  = (IndexID)(index_opt_log & 0x0fffffff);

            deconstruct_offset += sizeof(index_opt_log);
        }

        log_tuple_meta->logindex_lsn_ = start_lsn + deconstruct_offset;

        if (start_lsn / g_log_buffer_size == (start_lsn + deconstruct_offset) / g_log_buffer_size)
            log_tuple_meta->logindex_ptr_ = (MemPtr)txn_log_ + deconstruct_offset;
        else
            log_tuple_meta->logindex_ptr_ = replay_log_buf->buffer_mem_ptr_ + log_tuple_meta->logindex_lsn_ % g_log_buffer_size;
        

        //logindex meta: prev_log_buf_id  prev_log_lsn
        uint64_t log_index_meta = 0;
        log_index_meta = *(uint64_t*)(txn_log_ + deconstruct_offset);
        deconstruct_offset += sizeof(log_index_meta);

        log_tuple_meta->prev_log_buf_id_ = (log_index_meta & 0xff00000000000000) >> 56;
        log_tuple_meta->prev_log_lsn_    = (log_index_meta & 0x00ffffffffffffff);

        log_tuple_meta->prev_log_mem_ptr_ = 
                                        g_log_manager->log_buf_start_ptr_[log_tuple_meta->prev_log_buf_id_]
                                    + (log_tuple_meta->prev_log_lsn_ % g_log_buffer_size);

        // if (start_lsn / g_log_buffer_size != log_tuple_meta->prev_log_lsn_ / g_log_buffer_size)
        // {
        //     log_tuple_meta->prev_log_mem_ptr_ = 
        //                                     g_log_manager->log_buf_start_ptr_[log_tuple_meta->prev_log_buf_id_]
        //                                 + (log_tuple_meta->prev_log_lsn_ % g_log_buffer_size) + g_log_buffer_size;
        // }
        // else
        // {
        //     log_tuple_meta->prev_log_mem_ptr_ = 
        //                                     g_log_manager->log_buf_start_ptr_[log_tuple_meta->prev_log_buf_id_]
        //                                 + (log_tuple_meta->prev_log_lsn_ % g_log_buffer_size);
        // }


        //prev_log_ptr ao_mem_ptr
        deconstruct_offset += sizeof(uint64_t) * 2;

        //min_ts，在解析阶段设置min_ts
        memcpy(txn_log_ + deconstruct_offset, &log_tuple_meta->commit_ts_, sizeof(uint64_t));
        deconstruct_offset += sizeof(uint64_t);

        //max_ts
        deconstruct_offset += sizeof(uint64_t);

        //tuple log
        log_tuple_meta->tuple_log_lsn_ = start_lsn + deconstruct_offset;
        log_tuple_meta->tuple_log_ptr_ = (MemPtr)txn_log_ + (deconstruct_offset % g_log_buffer_size);
        deconstruct_offset += log_tuple_meta->tuple_log_size_;

        log_tuple_meta->end_lsn_ = start_lsn + deconstruct_offset;

        log_replay_context->log_tuple_num_++;
    }

}

void LogIndexLog::ReplayTxnLog(LogReplayContext* log_replay_context)
{
    LogBuffer* replayed_log_buf = log_replay_context->replay_log_buffer_;

    AMStrategy*        am_strategy    = log_replay_context->am_strategy_;
    LogIndexTupleMeta* log_tuple_meta = nullptr;



    while (!log_replay_context->unreplayed_log_tuple_.empty())
    {
        log_tuple_meta = log_replay_context->unreplayed_log_tuple_.front();
        
        replayed_log_buf->replaying_commit_ts_[log_replay_context->replay_context_index_] = log_tuple_meta->commit_ts_;

        if (am_strategy->ReplayTupleLog(log_tuple_meta, log_replay_context))
        {
            log_replay_context->unreplayed_log_tuple_.pop();
            delete log_tuple_meta;
            continue;
        }
        else
        {
            break;
        }
    }

    // if (log_replay_context->unreplayed_log_tuple_.empty())
    //     replayed_log_buf->replaying_commit_ts_[log_replay_context->replay_context_index_] = log_replay_context->log_txn_meta_->commit_ts_;


    for (uint64_t i = 0; i < log_replay_context->log_tuple_num_; i++)
    {
        log_tuple_meta = dynamic_cast<LogIndexTupleMeta*>(log_replay_context->log_tuple_meta_[i]);
        
        if (am_strategy->ReplayTupleLog(log_tuple_meta, log_replay_context))
        {
            ;
        }
        else
        {
            LogIndexTupleMeta* unreplayed_log_tuple = new LogIndexTupleMeta();
            unreplayed_log_tuple->CopyLogTupleMeta(dynamic_cast<LogIndexTupleMeta*>(log_tuple_meta));

            log_replay_context->unreplayed_log_tuple_.push(unreplayed_log_tuple);    
        }
    }

}

void LogIndexLog::FinishReplayTxn(LogReplayContext* log_replay_context)
{
    log_replay_context->ResetContext();
}


/***********************************************/
/**************** LogIndexTxnMeta **************/
/***********************************************/

LogIndexTxnMeta::LogIndexTxnMeta()
{
}

LogIndexTxnMeta::~LogIndexTxnMeta()
{
}




/***********************************************/
/************** LogIndexTupleMeta **************/
/***********************************************/

LogIndexTupleMeta::LogIndexTupleMeta()
{
}

LogIndexTupleMeta::~LogIndexTupleMeta()
{
}


void LogIndexTupleMeta::Reset()
{
    LogTupleMeta::Reset();

    prev_log_buf_id_  = 0;
    prev_log_lsn_     = 0;

    prev_log_mem_ptr_ = 0;
    
    logindex_lsn_ = 0;
    logindex_ptr_ = 0;
}


void LogIndexTupleMeta::CopyLogTupleMeta(LogIndexTupleMeta* log_tuple_meta)
{
    LogTupleMeta::CopyLogTupleMeta(log_tuple_meta);

    prev_log_buf_id_ = log_tuple_meta->prev_log_buf_id_;
    prev_log_lsn_    = log_tuple_meta->prev_log_lsn_;

    prev_log_mem_ptr_ = log_tuple_meta->prev_log_mem_ptr_;

    logindex_lsn_ = log_tuple_meta->logindex_lsn_;
    logindex_ptr_ = log_tuple_meta->logindex_ptr_;
}

#endif