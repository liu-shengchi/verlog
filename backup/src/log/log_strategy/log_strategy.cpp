#include "log_strategy.h"



/***************************************/
/************* LogStrategy *************/
/***************************************/

LogStrategy::LogStrategy()
{

}

LogStrategy::~LogStrategy()
{

}


/***************************************/
/************** LogTxnMeta *************/
/***************************************/

LogTxnMeta::LogTxnMeta()
{
    log_buf_id_ = 0;
    start_lsn_  = 0;

    log_size_     = 0;
    // log_buf_loop_ = false;
    // leave_size_   = 0;
    // temp_txn_log_ = nullptr;

    log_start_ptr_ = 0;
    // log_deconstruct_offset_ = 0;

    commit_ts_ = 0;
}

LogTxnMeta::~LogTxnMeta()
{
}


void LogTxnMeta::Reset()
{
    log_buf_id_ = 0;
    start_lsn_  = 0;

    // if (log_buf_loop_)
    // {
    //     free(temp_txn_log_);
    //     temp_txn_log_ = nullptr;
    // }
    
    log_size_     = 0;
    // log_buf_loop_ = false;
    // leave_size_   = 0;

    log_start_ptr_ = 0;
    // log_deconstruct_offset_ = 0;

    commit_ts_ = 0;
}



/****************************************/
/************* LogTupleMeta *************/
/****************************************/

LogTupleMeta::LogTupleMeta()
{
}

LogTupleMeta::~LogTupleMeta()
{
}


void LogTupleMeta::Reset()
{
    primary_key_ = 0;
    shard_id_    = 0;
    table_id_    = 0;
    opt_type_    = AccessType::READ_AT;
    tuple_log_size_ = 0;
    
    index_and_opt_cnt_ = 0;

    tuple_log_lsn_ = 0;
    tuple_log_ptr_ = 0;
    
    commit_ts_ = 0;
    
    log_logical_ptr_ = 0;
    log_buf_id_      = 0;
    start_lsn_       = 0;
    end_lsn_         = 0;
}

void LogTupleMeta::CopyLogTupleMeta(LogTupleMeta* log_tuple_meta)
{
    primary_key_ = log_tuple_meta->primary_key_;
    shard_id_    = log_tuple_meta->shard_id_;
    table_id_    = log_tuple_meta->table_id_;
    opt_type_    = log_tuple_meta->opt_type_;
    tuple_log_size_ = log_tuple_meta->tuple_log_size_;

    index_and_opt_cnt_ = log_tuple_meta->index_and_opt_cnt_;
    for (int i = 0; i < index_and_opt_cnt_; i++)
    {
        index_and_opts_[i].index_id_  = log_tuple_meta->index_and_opts_[i].index_id_;
        index_and_opts_[i].index_opt_ = log_tuple_meta->index_and_opts_[i].index_opt_;
    }
    
    tuple_log_lsn_ = log_tuple_meta->tuple_log_lsn_;
    tuple_log_ptr_ = log_tuple_meta->tuple_log_ptr_;
    
    commit_ts_     = log_tuple_meta->commit_ts_;

    log_logical_ptr_ = log_tuple_meta->log_logical_ptr_;
    log_buf_id_      = log_tuple_meta->log_buf_id_;
    start_lsn_       = log_tuple_meta->start_lsn_;
    end_lsn_         = log_tuple_meta->end_lsn_;
}