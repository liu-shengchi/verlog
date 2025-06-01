#include "taurus_log.h"

#include "txn_log.h"
#include "log_buffer.h"

#include "tpcc_schema.h"

#include "tuple.h"
#include "access_entry.h"

#include "txn_id.h"
#include "txn_context.h"

#include "string.h"


#if   LOG_STRATEGY_TYPE == TAURUS_LOG

/*********************************************/
/***************** TaurusLog *****************/
/*********************************************/
TaurusLog::TaurusLog()
{
}

TaurusLog::~TaurusLog()
{
}

    
void TaurusLog::AccessTuple(TxnContext* txn_context, AccessEntry* access_entry)
{
    TaurusTupleMeta* log_tuple_meta = access_entry->operate_tuple_->log_tuple_meta_;

    txn_context->log_txn_meta_->LVWiseMax(log_tuple_meta->write_lsn_vector_);

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    if (access_entry->access_type_ != READ_AT)
        txn_context->log_txn_meta_->LVWiseMax(log_tuple_meta->read_lsn_vector_);
#endif

}

void TaurusLog::CommitTuple(TxnContext* txn_context, AccessEntry* access_entry)
{

    TaurusTxnMeta* log_txn_meta = txn_context->log_txn_meta_;

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    
    /** 直接对原始元组更新日志元数据 **/
    TaurusTupleMeta* log_tuple_meta = access_entry->origin_tuple_->log_tuple_meta_;

    /* 
     * 对所有访问的元组，更新ReadLV
     * 对写操作（非只读操作）的元组，更新WriteLV
     */
    log_tuple_meta->ReadLVWiseMax(log_txn_meta->txn_lsn_vector_);
    if (access_entry->access_type_ != READ_AT)
        log_tuple_meta->WriteLVWiseMax(log_txn_meta->txn_lsn_vector_);

#elif CC_STRATEGY_TYPE == SILO_CC
    

#endif

}


void TaurusLog::CommitTuple(LogTupleMeta* log_tuple_meta, LogBufID log_buf_id, LogLSN log_lsn)
{
    TaurusTupleMeta* taurus_log_tuple_meta = dynamic_cast<TaurusTupleMeta*>(log_tuple_meta);

    taurus_log_tuple_meta->write_lsn_vector_[log_buf_id] += log_lsn;
}


uint64_t TaurusLog::ConstructTxnLog(TxnContext* txn_context, Timestamp commit_ts)
{
    /* 
     * 当前实现了基础的串行日志
     * 日志格式: 事务信息 + 元组修改信息1 + 元组修改信息2 + ... + 元组修改信息n  log item(bits)
     *   事务信息：
     *      txn_log_size (64) + commit_ts (64) + LSN向量(64 * g_log_buffer_num)
     *   元组修改信息：
     *      primary_key(64) + shardid(32) + tableid(16) + opt_type(4) + tuple_size(12) + tuple_data(..)
     * 
     * TODO: 
     *     记录每个元组修改的属性，用于辅助判断是否需要更新二级索引
     * 
     * 日志以元组为粒度，而非以事务为粒度。虽然同一事务的日志处在连续的日志空间，
     * 但是日志内并不维护事务信息，仅记录修改的元组相关信息。
     * 日志的LSN顺序和事务的可串行化顺序必须相同，保证恢复阶段按照日志顺序回放，能够恢复到数据库崩溃前的一致状态。
     */
    uint64_t log_size = 0;

    //存放事务本地日志
    TxnLog*  txn_log      = txn_context->txn_log_;
    uint64_t start_offset = txn_log->log_offset_;

    //预留事务日志size的空间，uint64_t
    txn_log->log_offset_ += sizeof(log_size);

    /* 
     * 如果需要支持备节点强一致读，在日志中记录commit_ts
     */
#if STRONGLY_CONSISTENCY_READ_ON_BACKUP
    memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&commit_ts, sizeof(commit_ts));
    txn_log->log_offset_ += sizeof(commit_ts);
#endif


    /** 在日志中写入lsn_vector **/
    TaurusTxnMeta* log_txn_meta = txn_context->log_txn_meta_;

    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&log_txn_meta->txn_lsn_vector_[i], sizeof(LogLSN));
        txn_log->log_offset_ += sizeof(LogLSN);
    }

    // for (uint64_t i = 0; i < txn_context->next_access_entry_; i++)
    // {
    //     AccessEntry* access = txn_context->txn_access_entry_[i];
        
    //     AccessType access_type = access->access_type_;
    //     if (access_type == READ_AT)
    //     {
    //         continue;
    //     }

    //     ShardID    shard_id    = access->shard_id_;
    //     TableID    table_id    = access->table_id_;
    //     TupleData  tuple_data  = access->operate_tuple_->tuple_data_;
        
    //     //需要注意delete操作的实现。
    //     //需要保证在delete操作下，Access的operate_tuple_中有保存元组数据。
    //     PrimaryKey primary_key = g_schema->GetIndexKey(table_id, tuple_data);
        
    //     uint64_t   opt_type = 0;    //仅使用低4位，用于标识 update、insert、delete
    //     uint64_t   tuple_size = 0;
    //     switch (access_type)
    //     {
    //     case UPDATE_AT:
    //         opt_type = 1<<access_type;
    //         tuple_size  = g_schema->GetTupleSize(table_id);
    //         break;
    //     case INSERT_AT:
    //         opt_type = 1<<access_type;
    //         tuple_size  = g_schema->GetTupleSize(table_id);
    //         break;
    //     case DELETE_AT:
    //         //删除操作不记录元组数据
    //         opt_type = 1<<access_type;
    //         tuple_size  = 0;
    //         break;
    //     default:
    //         break;
    //     }
        
    //     /* 
    //      * 将一些日志元数据合并为一个64bit整型
    //      * shardid: 32 - 63
    //      * tableid: 16 - 31
    //      * opt_type: 12 - 15
    //      * tuple_size: 0 - 11
    //      * 
    //      * 注意：要保证元组小于4kB（1<<12 Bytes）
    //      */
    //     uint64_t meta = 0;
    //     meta |= (uint64_t)shard_id << 32;
    //     meta |= (uint64_t)table_id << 16;
    //     meta |= opt_type << 12;
    //     meta |= tuple_size;

    //     memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&primary_key, sizeof(primary_key));
    //     txn_log->log_offset_ += sizeof(primary_key);

    //     memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&meta, sizeof(meta));
    //     txn_log->log_offset_ += sizeof(meta);

    //     memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)tuple_data, tuple_size);
    //     txn_log->log_offset_ += tuple_size;
    // }

    for (uint64_t i = 0; i < txn_context->next_access_entry_ / 1; i++)
    {
        AccessEntry* access = txn_context->txn_access_entry_[i];
        
        AccessType access_type = access->access_type_;
        if (access_type == READ_AT)
        {
            continue;
        }

        PrimaryKey primary_key = 0;
        
        uint64_t   shard_id    = access->shard_id_;
        uint64_t   table_id    = access->table_id_;
        uint64_t   opt_type       = 0;    //仅使用低4位，用于标识 update、insert、delete
        uint64_t   tuple_log_size = 0;
        uint64_t   index_and_opt_size = 0;
        
        TupleData  tuple_data  = access->operate_tuple_->tuple_data_;
        
        //需要注意delete操作的实现。
        //需要保证在delete操作下，Access的operate_tuple_中有保存元组数据。
        primary_key = g_schema->GetIndexKey(table_id, tuple_data);
        
        switch (access_type)
        {
        case UPDATE_AT:
            opt_type        = 1LU<<access_type;
            tuple_log_size  = g_schema->GetTupleSize(table_id);
            break;
        case INSERT_AT:
            opt_type        = 1LU<<access_type;
            tuple_log_size  = g_schema->GetTupleSize(table_id);
            break;
        case DELETE_AT:
            //删除操作不记录元组数据
            opt_type        = 1LU<<access_type;
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
         * tuple_log_size: 12 - 27
         * index_and_opt_size 0 -11
         */
        uint64_t meta = 0;
        meta |= ((uint64_t)shard_id) << 48;
        meta |= ((uint64_t)table_id) << 32;
        meta |= ((uint64_t)opt_type) << 28;
        meta |= ((uint64_t)tuple_log_size) << 12;
        meta |= (uint64_t)index_and_opt_size;
        
        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&primary_key, sizeof(primary_key));
        txn_log->log_offset_ += sizeof(primary_key);

        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&meta, sizeof(meta));
        txn_log->log_offset_ += sizeof(meta);

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

        memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)tuple_data, tuple_log_size);
        txn_log->log_offset_ += tuple_log_size;
    }



    log_size = txn_log->log_offset_ - start_offset;

    memcpy(txn_log->txn_log_buffer_ + start_offset, (char*)&log_size, sizeof(log_size));

    return log_size;
}


uint64_t TaurusLog::ConstructLoadLog(
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
     * 如果需要支持备节点强一致读
     */
#if STRONGLY_CONSISTENCY_READ_ON_BACKUP
    memcpy(log_entry + log_offset, (char*)&commit_ts, sizeof(commit_ts));
    log_offset += sizeof(commit_ts);
#endif

    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        memset(log_entry + log_offset, 0x00, sizeof(LogLSN));
        log_offset += sizeof(LogLSN);
    }

    PrimaryKey primary_key = 0;
    
    uint64_t   opt_type       = 0;    //仅使用低4位，用于标识 update、insert、delete
    uint64_t   tuple_log_size = 0;
    uint64_t   index_and_opt_size = 0;
    
    TupleData  tuple_data  = tuple->tuple_data_;
    
    //需要注意delete操作的实现。
    primary_key = g_schema->GetIndexKey(table_id, tuple_data);
    
    switch (access_type)
    {
    case UPDATE_AT:
        opt_type        = 1LU<<access_type;
        tuple_log_size  = g_schema->GetTupleSize(table_id);
        break;
    case INSERT_AT:
        opt_type        = 1LU<<access_type;
        tuple_log_size  = g_schema->GetTupleSize(table_id);
        break;
    case DELETE_AT:
        //删除操作不记录元组数据
        opt_type        = 1LU<<access_type;
        tuple_log_size  = 0;
        break;
    default:
        break;
    }
    
    //每个index_and_opt记录在日志中占4字节
    index_and_opt_size = index_and_opt_cnt * 4;
    
    /* 
        * 将一些日志元数据合并为一个64bit整型
        * shardid: 48 - 63
        * tableid: 32 - 47
        * opt_type: 28 - 31
        * tuple_log_size: 12 - 27
        * index_and_opt_size 0 -11
        */
    uint64_t meta = 0;
    meta |= ((uint64_t)shard_id) << 48;
    meta |= ((uint64_t)table_id) << 32;
    meta |= opt_type       << 28;
    meta |= tuple_log_size << 12;
    meta |= index_and_opt_size;
    
    memcpy(log_entry + log_offset, (char*)&primary_key, sizeof(primary_key));
    log_offset += sizeof(primary_key);

    memcpy(log_entry + log_offset, (char*)&meta, sizeof(meta));
    log_offset += sizeof(meta);

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

    memcpy(log_entry + log_offset, (char*)tuple_data, tuple_log_size);
    log_offset += tuple_log_size;

    txn_log_size = log_offset;

    COMPILER_BARRIER

    memcpy(log_entry, (char*)&txn_log_size, sizeof(txn_log_size));
    
    return txn_log_size;
}

/*********************************************/
/************** TaurusTupleMeta **************/
/*********************************************/
TaurusTupleMeta::TaurusTupleMeta()
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        write_lsn_vector_[i] = 0;
        read_lsn_vector_[i]  = 0;
    }
}

TaurusTupleMeta::~TaurusTupleMeta()
{
}


void TaurusTupleMeta::Reset()
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        write_lsn_vector_[i] = 0;
        read_lsn_vector_[i]  = 0;
    }
}


void TaurusTupleMeta::CopyLogTupleMeta(LogTupleMeta* log_tuple_meta)
{
    TaurusTupleMeta* source_log_meta = dynamic_cast<TaurusTupleMeta*>(log_tuple_meta);

    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        write_lsn_vector_[i] = source_log_meta->write_lsn_vector_[i];
        read_lsn_vector_[i]  = source_log_meta->read_lsn_vector_[i];
    }
}


void TaurusTupleMeta::ReadLVWiseMax(LogLSN lsn_verctor[])
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
        if (read_lsn_vector_[i] < lsn_verctor[i])
            read_lsn_vector_[i] = lsn_verctor[i];
}


void TaurusTupleMeta::WriteLVWiseMax(LogLSN lsn_verctor[])
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
        if (write_lsn_vector_[i] < lsn_verctor[i])
            write_lsn_vector_[i] = lsn_verctor[i];
}



/*********************************************/
/*************** TaurusTxnMeta ***************/
/*********************************************/
TaurusTxnMeta::TaurusTxnMeta()
{    
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        txn_lsn_vector_[i] = 0;
    }
}

TaurusTxnMeta::~TaurusTxnMeta()
{
}


void TaurusTxnMeta::LVWiseMax(LogLSN lsn_verctor[])
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
        if (txn_lsn_vector_[i] < lsn_verctor[i])
            txn_lsn_vector_[i] = lsn_verctor[i];
}


void TaurusTxnMeta::Reset()
{
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        txn_lsn_vector_[i] = 0;
    }
}


#endif