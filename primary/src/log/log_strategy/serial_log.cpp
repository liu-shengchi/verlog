#include "serial_log.h"

#include "txn_log.h"
#include "log_buffer.h"

#include "tpcc_schema.h"
#include "tuple.h"

#include "txn_id.h"
#include "txn_context.h"

#include "access_entry.h"

#include "string.h"


using namespace std;


#if   LOG_STRATEGY_TYPE == SERIAL_LOG

/*********************************************/
/***************** SerialLog *****************/
/*********************************************/

SerialLog::SerialLog()
{

}

SerialLog::~SerialLog()
{
    
}

    
void SerialLog::AccessTuple(TxnContext* txn_context, AccessEntry* access_entry)
{

}


void SerialLog::CommitTuple(TxnContext* txn_context, AccessEntry* access_entry)
{
    
}


void SerialLog::CommitTuple(LogTupleMeta* log_tuple_meta, LogBufID log_buf_id, LogLSN log_lsn)
{

}

uint64_t SerialLog::ConstructTxnLog(TxnContext* txn_context, Timestamp commit_ts)
{
    /* 
     * 当前实现了基础的串行日志
     * 日志格式: 事务信息 + 元组修改信息1 + 元组修改信息2 + ... + 元组修改信息n  log item(bits)
     *   事务信息：
     *        txn_log_size (64) 
     *      + commit_ts (64)
     *   元组修改信息：
     *        //元组的主键
     *      + primary_key(64)
     *      
     *        //shardid  表示元组所在分区 （系统不能超过 2^16-1 个分区） 
     *        //tableid  表示元组所在表 （系统不能超过 2^16-1 张表）
     *        //opt_type 元组操作的类型：update(0010) insert(0100) delete(1000)
     *        //tuple_log_size 表示该元组生成日志的大小 （单个元组日志不能超过 2^16-1 大小）
     *        //index_and_opt_size 表示记录索引以及索引操作的类型所占的日志空间 (单个元组日志包含的数据修改，不能超过 2^8-1 个索引的修改)
     *      + shardid(16) + tableid(16) + opt_type(4) + tuple_log_size(16)+ index_and_opt_size(12)
     * 
     *        //每个索引和操作记录，由32位/4字节组成，高4位编码索引操作类型 insert(0100) delete(1000)
     *        //低28位编码索引ID.（数据库系统不能超过 2^28-1 个索引）
     *      + index_and_opt_info_1(32) + index_and_opt_info_2(32) + ... + index_and_opt_info_n(32)
     *        
     *        //记录元组修改日志
     *      + tuple_log(..)
     * 
     * 
     * 日志以元组为粒度，而非以事务为粒度。虽然同一事务的日志处在连续的日志空间，
     * 但是日志内并不维护事务信息，仅记录修改的元组相关信息。
     * 日志的LSN顺序和事务的可串行化顺序必须相同，保证恢复阶段按照日志顺序回放，能够恢复到数据库崩溃前的一致状态。
     */
    uint64_t txn_log_size = 0;
     
    //存放事务本地日志
    TxnLog*  txn_log      = txn_context->txn_log_;
    txn_log->log_offset_  = 0;
    uint64_t start_offset = 0;
    
    //预留事务日志size的空间，uint64_t
    txn_log->log_offset_ += sizeof(txn_log_size);

    /* 
     * 如果需要支持备节点强一致读
     */
#if STRONGLY_CONSISTENCY_READ_ON_BACKUP
    memcpy(txn_log->txn_log_buffer_ + txn_log->log_offset_, (char*)&commit_ts, sizeof(commit_ts));
    txn_log->log_offset_ += sizeof(commit_ts);
#endif

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
    
    txn_log_size = txn_log->log_offset_ - start_offset;
    
    COMPILER_BARRIER

    memcpy(txn_log->txn_log_buffer_ + start_offset, (char*)&txn_log_size, sizeof(txn_log_size));

    return txn_log_size;
}


uint64_t SerialLog::ConstructLoadLog(
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
/************** SerialTupleMeta **************/
/*********************************************/

SerialTupleMeta::SerialTupleMeta()
{
    
}

SerialTupleMeta::~SerialTupleMeta()
{
}

void SerialTupleMeta::Reset()
{

}

void SerialTupleMeta::CopyLogTupleMeta(LogTupleMeta* log_tuple_meta)
{

}



/*********************************************/
/*************** SerialTxnMeta ***************/
/*********************************************/

SerialTxnMeta::SerialTxnMeta()
{
}

SerialTxnMeta::~SerialTxnMeta()
{
}


void SerialTxnMeta::Reset()
{

}


#endif