#include "logindex_log.h"

#include "txn_log.h"
#include "log_buffer.h"

#include "tpcc_schema.h"

#include "tuple.h"
#include "access_entry.h"

#include "txn_id.h"
#include "txn_context.h"

#include "string.h"


#if    LOG_STRATEGY_TYPE == LOGINDEX_LOG

/***********************************************/
/***************** LogIndexLog *****************/
/***********************************************/

LogIndexLog::LogIndexLog()
{
}

LogIndexLog::~LogIndexLog()
{
}


void LogIndexLog::AccessTuple(TxnContext* txn_context, AccessEntry* access_entry)
{
    if (access_entry->access_type_ == READ_AT)
        return;
    
    LogIndexTxnMeta* log_txn_meta = txn_context->log_txn_meta_;

    log_txn_meta->write_access_index_[log_txn_meta->write_access_cnt_] = access_entry->access_entry_index_;
    log_txn_meta->write_access_cnt_++;
}


void LogIndexLog::CommitTuple(TxnContext* txn_context, AccessEntry* access_entry)
{
    LogIndexTxnMeta*   log_txn_meta   = txn_context->log_txn_meta_;
    
    uint64_t           access_index   = 0;
    LogIndexTupleMeta* log_tuple_meta = nullptr;

    for (uint64_t i = 0; i < log_txn_meta->write_access_cnt_; i++)
    {
        access_index   = log_txn_meta->write_access_index_[i];
        access_entry   = txn_context->txn_access_entry_[access_index];
        /* 获取operate_tuple的log_tuple_meta */
        log_tuple_meta = access_entry->operate_tuple_->log_tuple_meta_;

        /* 
         * 修改log_tuple_meta
         */
        log_tuple_meta->log_buf_id_ = log_txn_meta->log_buf_id_;
        log_tuple_meta->log_lsn_    = txn_context->start_log_lsn_ + log_txn_meta->write_access_lsn_offset_[i];
    }
}


void LogIndexLog::CommitTuple(LogTupleMeta* log_tuple_meta, LogBufID log_buf_id, LogLSN log_lsn)
{
    LogIndexTupleMeta* logindex_tuple_meta = dynamic_cast<LogIndexTupleMeta*>(log_tuple_meta);

    logindex_tuple_meta->log_buf_id_ = log_buf_id;
    logindex_tuple_meta->log_lsn_ += log_lsn;
}


uint64_t LogIndexLog::ConstructTxnLog(TxnContext* txn_context, Timestamp commit_ts)
{
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
              //标识同元组的上一次更新日志所在的位置
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
    uint64_t txn_log_size = 0;

    //存放事务本地日志
    TxnLog*  txn_log      = txn_context->txn_log_;
    txn_log->log_offset_  = 0;
    uint64_t start_offset = 0;


    //预留事务日志size的空间，uint64_t
    txn_log->log_offset_ += sizeof(txn_log_size);
    
    /* 
     * 如果需要支持备节点强一致读，在日志中记录commit_ts
     */
#if STRONGLY_CONSISTENCY_READ_ON_BACKUP
    memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&commit_ts, sizeof(commit_ts));
    txn_log->log_offset_ += sizeof(commit_ts);
#endif

    LogIndexTxnMeta* log_txn_meta = txn_context->log_txn_meta_;

    for (uint64_t i = 0; i < log_txn_meta->write_access_cnt_; i++)
    {
        uint64_t access_index = log_txn_meta->write_access_index_[i];

        AccessEntry* access = txn_context->txn_access_entry_[access_index];
        
        AccessType access_type = access->access_type_;
        if (access_type == READ_AT)
        {
            continue;
        }

        LogIndexTupleMeta* log_tuple_meta = access->operate_tuple_->log_tuple_meta_;

        PrimaryKey primary_key = 0;

        LogBufID prev_log_buf_id = log_tuple_meta->log_buf_id_;
        LogLSN   prev_log_lsn    = log_tuple_meta->log_lsn_;

        ShardID    shard_id   = access->shard_id_;
        TableID    table_id   = access->table_id_;
        uint64_t   opt_type       = 0;    //仅使用低4位，用于标识 update、insert、delete
        uint64_t   tuple_log_size = 0;
        uint64_t   index_and_opt_size = 0;

        TupleData  tuple_data = access->operate_tuple_->tuple_data_;

        //需要注意delete操作的实现。
        //需要保证在delete操作下，Access的operate_tuple_中有保存元组数据。
        primary_key = g_schema->GetIndexKey(table_id, tuple_data);

        //primary_key 64bits
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&primary_key, sizeof(primary_key));
        txn_log->log_offset_ += sizeof(primary_key);

        switch (access_type)
        {
        case UPDATE_AT:
            opt_type = 1LU<<access_type;
            tuple_log_size  = g_schema->GetTupleSize(table_id);
            break;
        case INSERT_AT:
            opt_type = 1LU<<access_type;
            tuple_log_size  = g_schema->GetTupleSize(table_id);
            break;
        case DELETE_AT:
            opt_type = 1LU<<access_type;
            //删除操作不记录元组数据
            tuple_log_size  = 0;
            break;
        default:
            break;
        }
        
        //每个index_and_opt记录在日志中占4字节
        index_and_opt_size = access->index_and_opt_cnt_ * 4;

        /* 
         * 将一些日志元数据合并为一个64bit整型
         * shardid: 48 - 63
         * tableid: 32 - 47
         * opt_type: 28 - 31
         * tuple_size: 12 - 27 要保证元组小于(64KB -1)
         * index_and_opt_size 0 -11
         */
        uint64_t access_meta = 0;
        access_meta |= ((uint64_t)shard_id) << 48;
        access_meta |= ((uint64_t)table_id) << 32;
        access_meta |= ((uint64_t)opt_type) << 28;
        access_meta |= ((uint64_t)tuple_log_size) << 12;
        access_meta |= (uint64_t)index_and_opt_size;

        //access_meta 64bits
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&access_meta, sizeof(access_meta));
        txn_log->log_offset_ += sizeof(access_meta);

        uint32_t index_and_opt_log = 0;
        for (int i = 0; i < access->index_and_opt_cnt_; i++)
        {
            uint32_t index_opt = (uint32_t)access->index_and_opts_[i].index_opt_;
            uint32_t index_id  = (uint32_t)access->index_and_opts_[i].index_id_;
            
            index_and_opt_log |= (index_opt << 28);
            index_and_opt_log |= index_id;
            

            memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&index_and_opt_log, sizeof(index_and_opt_log));
            txn_log->log_offset_ += sizeof(index_and_opt_log);
        }
        
        //设置日志索引偏移量
        log_txn_meta->write_access_lsn_offset_[i] = txn_log->log_offset_;
        
        uint64_t log_index_meta  = 0;
        log_index_meta |= (uint64_t)prev_log_buf_id << 56;
        log_index_meta |= (uint64_t)prev_log_lsn;

        //log_index_meta (prev_log_buf_id / prev_log_lsn) 64bits
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&log_index_meta, sizeof(log_index_meta));
        txn_log->log_offset_ += sizeof(log_index_meta);

        //空余sizeof(uint64_t) * 2，用于日志回放阶段构造日志索引
        memset(txn_log->txn_log_buffer_ + txn_log->log_offset_, 0, sizeof(uint64_t) * 2);
        txn_log->log_offset_ += sizeof(uint64_t) * 2;

        //min_ts
        memset(txn_log->txn_log_buffer_ + txn_log->log_offset_, 0, sizeof(uint64_t));
        txn_log->log_offset_ += sizeof(uint64_t);

        //max_ts
        memset(txn_log->txn_log_buffer_ + txn_log->log_offset_, 0, sizeof(uint64_t));
        txn_log->log_offset_ += sizeof(uint64_t);

        //tuple log
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)tuple_data, tuple_log_size);
        txn_log->log_offset_ += tuple_log_size;
    }
    
    txn_log_size = txn_log->log_offset_ - start_offset;

    memcpy(txn_log->txn_log_buffer_ + start_offset, (char*)&txn_log_size, sizeof(txn_log_size));

    return txn_log_size;
}


uint64_t LogIndexLog::ConstructLoadLog(
                        char* log_entry, 
                        LogBufID log_buf_id, 
                        Tuple* tuple, 
                        AccessType access_type, 
                        ShardID shard_id, 
                        TableID table_id, 
                        uint64_t index_and_opt_cnt, 
                        IndexAndOpt index_and_opts[],
                        Timestamp commit_ts,
                        LogTupleMeta* log_tuple_meta)
{
    uint64_t txn_log_size = 0;

    //日志偏移量
    uint64_t log_offset = 0;

    //预留事务日志size的空间，uint64_t
    log_offset += sizeof(txn_log_size);
    
    /* 
     * 如果需要支持备节点强一致读，在日志中记录commit_ts
     */
#if STRONGLY_CONSISTENCY_READ_ON_BACKUP
    memcpy(log_entry + log_offset, (char*)&commit_ts, sizeof(commit_ts));
    log_offset += sizeof(commit_ts);
#endif


    PrimaryKey primary_key = 0;

    //在Load阶段，access_type一定为 insert_at 
    LogBufID prev_log_buf_id = 0;
    LogLSN   prev_log_lsn    = 0;

    uint64_t   opt_type       = 0;    //仅使用低4位，用于标识 update、insert、delete
    uint64_t   tuple_log_size = 0;
    uint64_t   index_and_opt_size = 0;

    TupleData  tuple_data = tuple->tuple_data_;

    //需要注意delete操作的实现。
    //需要保证在delete操作下，Access的operate_tuple_中有保存元组数据。
    primary_key = g_schema->GetIndexKey(table_id, tuple_data);
    

    opt_type = 1LU<<access_type;
    tuple_log_size  = g_schema->GetTupleSize(table_id);
    
    //每个index_and_opt记录在日志中占4字节
    index_and_opt_size = index_and_opt_cnt * 4;

    /* 
        * 将一些日志元数据合并为一个64bit整型
        * shardid: 48 - 63
        * tableid: 32 - 47
        * opt_type: 28 - 31
        * tuple_size: 12 - 27 要保证元组小于(64KB -1)
        * index_and_opt_size 0 -11
        */
    uint64_t access_meta = 0;
    access_meta |= ((uint64_t)shard_id) << 48;
    access_meta |= ((uint64_t)table_id) << 32;
    access_meta |= ((uint64_t)opt_type) << 28;
    access_meta |= ((uint64_t)tuple_log_size) << 12;
    access_meta |= (uint64_t)index_and_opt_size;

    uint64_t log_index_meta  = 0;
    log_index_meta |= (uint64_t)prev_log_buf_id << 56;
    log_index_meta |= (uint64_t)prev_log_lsn;

    //primary_key 64bits
    memcpy(log_entry + log_offset, (char*)&primary_key, sizeof(primary_key));
    log_offset += sizeof(primary_key);

    //access_meta 64bits
    memcpy(log_entry + log_offset, (char*)&access_meta, sizeof(access_meta));
    log_offset += sizeof(access_meta);

    uint32_t index_and_opt_log = 0;
    for (int i = 0; i < index_and_opt_cnt; i++)
    {
        uint32_t index_opt = (uint32_t)index_and_opts[i].index_opt_;
        uint32_t index_id  = (uint32_t)index_and_opts[i].index_id_;
        
        index_and_opt_log |= (index_opt << 28);
        index_and_opt_log |= index_id;
        
        memcpy(log_entry + log_offset, (char*)&index_and_opt_log, sizeof(index_and_opt_log));
        log_offset += sizeof(index_and_opt_log);
    }
    
    //设置日志索引偏移量
    ((LogIndexTupleMeta*)log_tuple_meta)->log_buf_id_ = log_buf_id;
    ((LogIndexTupleMeta*)log_tuple_meta)->log_lsn_    = log_offset;

    //log_index_meta 64bits
    memcpy(log_entry + log_offset, (char*)&log_index_meta, sizeof(log_index_meta));
    log_offset += sizeof(log_index_meta);

    //空余sizeof(uint64_t) * 2，用于日志回放阶段构造日志索引
    memset(log_entry + log_offset, 0, sizeof(uint64_t) * 2);
    log_offset += sizeof(uint64_t) * 2;

    //min_ts
    memset(log_entry + log_offset, 0, sizeof(uint64_t));
    log_offset += sizeof(uint64_t);

    //max_ts
    memset(log_entry + log_offset, 0, sizeof(uint64_t));
    log_offset += sizeof(uint64_t);

    memcpy(log_entry + log_offset, (char*)tuple_data, tuple_log_size);
    log_offset += tuple_log_size;

    COMPILER_BARRIER
    txn_log_size = log_offset;
    COMPILER_BARRIER
    
    memcpy(log_entry, (char*)&txn_log_size, sizeof(txn_log_size));

    return txn_log_size;
}



/****************************************************/
/***************** LogIndexTupleLog *****************/
/****************************************************/

LogIndexTupleMeta::LogIndexTupleMeta()
{
    log_buf_id_ = 0;
    log_lsn_    = 0;
}

LogIndexTupleMeta::~LogIndexTupleMeta()
{
}


void LogIndexTupleMeta::CopyLogTupleMeta(LogTupleMeta* log_tuple_meta)
{
    LogIndexTupleMeta* source_log_meta = dynamic_cast<LogIndexTupleMeta*>(log_tuple_meta);

    log_buf_id_ = source_log_meta->log_buf_id_;
    log_lsn_    = source_log_meta->log_lsn_;
}


void LogIndexTupleMeta::Reset()
{
    log_buf_id_ = 0;
    log_lsn_    = 0;
}

/**************************************************/
/***************** LogIndexTxnLog *****************/
/**************************************************/

LogIndexTxnMeta::LogIndexTxnMeta()
{
    log_buf_id_       = 0;
    write_access_cnt_ = 0;

    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
    {
        write_access_index_[i]      = 0;
        write_access_lsn_offset_[i] = 0;   
    }
}

LogIndexTxnMeta::~LogIndexTxnMeta()
{
}


void LogIndexTxnMeta::Reset()
{
    write_access_cnt_ = 0;
}

#endif