#include "serial_log.h"

#include "log_buffer.h"

#include "log_replay_context.h"

#include "am_strategy.h"

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


void SerialLog::DeconstructTxnLog(LogReplayContext* log_replay_context)
{
    SerialLogTxnMeta*   log_txn_meta   = dynamic_cast<SerialLogTxnMeta*>(log_replay_context->log_txn_meta_);
    SerialLogTupleMeta* log_tuple_meta = nullptr;

    uint64_t txn_log_size  = log_txn_meta->log_size_;
    LogLSN   start_lsn     = log_txn_meta->start_lsn_;

    /**
     * 后续需要对日志进行解析操作，
     * 因此需要确定日志所在的连续空间的起始地址。
     * 1. 如果事务日志没有跨日志缓冲区
     *     txn_log_指向日志缓冲区中的日志
     * 2. 事务跨日志缓冲区
     *     txn_log_指向临时缓冲区的日志
     */
    char* txn_log_ = (char*)log_txn_meta->log_start_ptr_;

    log_txn_meta->log_buf_id_ = log_replay_context->replay_log_buf_id_;

    uint64_t deconstruct_offset = 0;

    /**
     * 解析事务日志记录
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
     * **/
    /* 解析事务日志的元数据 */
    //txn_log_size
    deconstruct_offset += 8;

    //commit_ts
    log_txn_meta->commit_ts_ = *(Timestamp*)(txn_log_ + deconstruct_offset);
    deconstruct_offset += sizeof(Timestamp);

    /**
     * 解析日志记录的元组
     * **/
    log_replay_context->log_tuple_num_ = 0;
    while (deconstruct_offset < txn_log_size)
    {
        log_tuple_meta = (SerialLogTupleMeta*)log_replay_context->log_tuple_meta_[log_replay_context->log_tuple_num_];

        log_tuple_meta->commit_ts_  = log_txn_meta->commit_ts_;

        log_tuple_meta->log_buf_id_ = log_txn_meta->log_buf_id_;
        log_tuple_meta->start_lsn_  = start_lsn + deconstruct_offset;

        //primary_key
        log_tuple_meta->primary_key_ = *(PrimaryKey*)(txn_log_ + deconstruct_offset);
        deconstruct_offset += sizeof(PrimaryKey);
        
        //shard_id, table_id, opt_type, tuple_log_size, index_and_opt_size
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

        for (int i = 0; i < log_tuple_meta->index_and_opt_cnt_; i++)
        {
            uint32_t index_opt_log = 0;
            index_opt_log = *(uint32_t*)(txn_log_ + deconstruct_offset);
            deconstruct_offset += sizeof(index_opt_log);

            log_tuple_meta->index_and_opts_[i].index_id_  = (IndexID)(index_opt_log & 0x0fffffff);
            log_tuple_meta->index_and_opts_[i].index_opt_ = (AccessType)((index_opt_log & 0xf0000000) >> 28);
        }

        //tuple log 
        log_tuple_meta->tuple_log_lsn_ = start_lsn + deconstruct_offset;
        log_tuple_meta->tuple_log_ptr_ = (MemPtr)txn_log_ + deconstruct_offset;
        deconstruct_offset += log_tuple_meta->tuple_log_size_;

        log_tuple_meta->end_lsn_ = start_lsn + deconstruct_offset;

        log_replay_context->log_tuple_num_++;
    }
    
}


void SerialLog::ReplayTxnLog(LogReplayContext* log_replay_context)
{   
    AMStrategy*   am_strategy    = log_replay_context->am_strategy_;

    LogTxnMeta*   log_txn_meta   = log_replay_context->log_txn_meta_;
    LogTupleMeta* log_tuple_meta = nullptr;

    for (uint64_t i = 0; i < log_replay_context->log_tuple_num_; i++)
    {
        log_tuple_meta = log_replay_context->log_tuple_meta_[i];
        am_strategy->ReplayTupleLog(log_tuple_meta);
    }
}


void SerialLog::FinishReplayTxn(LogReplayContext* log_replay_context)
{
    log_replay_context->ResetContext();
}


/*********************************************/
/************* SerialLogTxnMeta **************/
/*********************************************/

SerialLogTxnMeta::SerialLogTxnMeta()
{
}

SerialLogTxnMeta::~SerialLogTxnMeta()
{
}



/*********************************************/
/************* SerialLogTxnMeta **************/
/*********************************************/

SerialLogTupleMeta::SerialLogTupleMeta()
{
}

SerialLogTupleMeta::~SerialLogTupleMeta()
{
}


#endif