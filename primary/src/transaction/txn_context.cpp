#include "txn_context.h"


#include "txn_id.h"
#include "txn_thread.h"

#include "tpcc_workload.h"

#include "access_entry.h"

#include "s2pl_waitdie.h"
#include "silo.h"
#include "mvto.h"

#include "log_manager.h"
#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"

#include "txn_log.h"
#include "log_buffer.h"

#include "statistic_manager.h"

#include "db_rw_lock.h"


#include "string.h"



TxnContext::TxnContext()
{
    txn_id_      = nullptr;
    current_txn_ = nullptr;

    next_access_entry_ = 0;
    for (uint64_t i = 0; i < MAX_TUPLE_ACCESS_PER_TXN; i++)
        txn_access_entry_[i] = nullptr;
    
    cc_strategy_       = nullptr;
    cc_txn_meta_       = nullptr;


    log_strategy_ = nullptr;
    log_txn_meta_ = nullptr;

    txn_log_      = new TxnLog(MAX_TXN_LOG_SIZE);
    log_buf_id_   = 0;
    log_buffer_   = nullptr;

#if DISTRIBUTED_LOG == true
    remote_txn_log_    = new TxnLog(MAX_TXN_LOG_SIZE);
    remote_log_buffer_ = nullptr;
#endif

}

TxnContext::~TxnContext()
{

}


void TxnContext::InitContext(TxnThread* txn_thread, CCStrategy* cc_strategy, LogStrategy* log_strategy, LogBuffer* log_buffer, LogBuffer* remote_log_buffer)
{
    txn_thread_ = txn_thread;

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    cc_strategy_ = dynamic_cast<S2plWDStrategy*>(cc_strategy);
    cc_txn_meta_ = new S2plWDTxnMeta();
    for (uint64_t i = 0; i < MAX_TUPLE_ACCESS_PER_TXN; i++)
    {
        txn_access_entry_[i] = new S2plWDAccess();
    }
#elif CC_STRATEGY_TYPE == SILO_CC
    cc_strategy_ = dynamic_cast<SiloStrategy*>(cc_strategy);
    cc_txn_meta_ = new SiloTxnMeta();
    for (uint64_t i = 0; i < MAX_TUPLE_ACCESS_PER_TXN; i++)
    {
        txn_access_entry_[i] = new SiloAccess();
    }
#elif CC_STRATEGY_TYPE == MVTO_CC
    cc_strategy_ = dynamic_cast<MvtoStrategy*>(cc_strategy);
    cc_txn_meta_ = new MvtoTxnMeta();
    for (uint64_t i = 0; i < MAX_TUPLE_ACCESS_PER_TXN; i++)
    {
        txn_access_entry_[i] = new MvtoAccess();
    }
#endif

    log_buf_id_  = log_buffer->log_buffer_id_;
    log_buffer_  = log_buffer;

#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    log_strategy_ = dynamic_cast<SerialLog*>(log_strategy);
    log_txn_meta_ = new SerialTxnMeta();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    log_strategy_ = dynamic_cast<TaurusLog*>(log_strategy);
    log_txn_meta_ = new TaurusTxnMeta();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    log_strategy_ = dynamic_cast<LogIndexLog*>(log_strategy);
    log_txn_meta_ = new LogIndexTxnMeta();
    log_txn_meta_->log_buf_id_ = log_buf_id_;
#endif


#if DISTRIBUTED_LOG == true
    remote_buffer_ = remote_log_buffer;
#endif
}


void TxnContext::InitTxn(Transaction* txn)
{
    txn_id_      = txn->GetTxnIdentifier();
    current_txn_ = txn;

    current_txn_->SetTxnContext(this);

    cc_strategy_->BeginTxn(this);
}


RC TxnContext::AccessTuple(AccessType access_type, TableID table_id, ShardID shard_id, uint64_t index_and_cnt, IndexAndOpt index_and_opts[], Tuple* origin_tuple, Tuple* &operate_tuple)
{
    RC rc = RC::RC_OK;

    AccessEntry* access_entry = txn_access_entry_[next_access_entry_];

    /** 对元组进行必要的并发控制操作 **/
    rc = cc_strategy_->AccessTuple(cc_txn_meta_, access_entry, access_type, table_id, shard_id, origin_tuple, operate_tuple);
    

    if (rc == RC_OK)
    {
        access_entry->index_and_opt_cnt_ = index_and_cnt;
        for (size_t i = 0; i < index_and_cnt; i++)
        {
            access_entry->index_and_opts_[i].index_id_  = index_and_opts[i].index_id_;
            access_entry->index_and_opts_[i].index_opt_ = index_and_opts[i].index_opt_;
        }

        access_entry->access_entry_index_ = next_access_entry_;

        next_access_entry_++;

#if !NO_LOG
        /** 对元组进行日志相关操作 **/
        log_strategy_->AccessTuple(this, access_entry);
#endif

    }
    
    return rc;
}


RC TxnContext::RunTxn()
{
    return current_txn_->RunTxn();
}

RC TxnContext::FinishTxn(RC rc)
{
    if (rc == RC::RC_COMMIT)
        rc = cc_strategy_->PrepareTxn(this);
    
#if   TWO_PHASE_COMMIT
        //2pc, for distrbuted transctions
#endif


    if (rc == RC::RC_COMMIT)
    {
#if !NO_LOG

//   #if   !THREAD_BIND_SHARD
//     ShardID shard_id = current_txn_->local_shard_id_;
//     log_buf_id_      = shard_id % g_log_buffer_num;
//     log_buffer_      = g_log_manager->GetLogBuffer(log_buf_id_);
//   #endif

    #if STRONGLY_CONSISTENCY_READ_ON_BACKUP
        Timestamp commit_ts = 0;

        /** 构造事务日志 **/
        log_size_ = log_strategy_->ConstructTxnLog(this, commit_ts);
        
        // log_buffer_->filling_[txn_id_->thread_id_] = true;
        // COMPILER_BARRIER

        /* 
         * 需要对日志缓冲区上锁，保证同一日志缓冲区内，
         * 事务日志顺序等于事务提交时间戳顺序（可串行化顺序）
         */
        log_buffer_->log_buf_spinlock_.GetSpinLock();

        commit_ts = ATOM_FETCH_ADD(g_commit_ts, 1);
        /** 向日志流获取日志起始LSN **/
        start_log_lsn_ = log_buffer_->AtomicFetchLSN(log_size_);
        
        //填充commit_ts
        memcpy(txn_log_->txn_log_buffer_ + sizeof(uint64_t), (char*)&commit_ts, sizeof(commit_ts));

    #else
        /** 构造事务日志 **/
        log_size_ = log_strategy_->ConstructTxnLog(this);

        // log_buffer_->filling_[txn_id_->thread_id_] = true;
        // COMPILER_BARRIER

        log_buffer_->log_buf_spinlock_.GetSpinLock();

        /** 向日志流获取日志起始LSN **/
        start_log_lsn_  = log_buffer_->AtomicFetchLSN(log_size_);
    #endif

        COMPILER_BARRIER
        /** 将本地事务日志同步到日志缓冲区中 **/
        log_buffer_->SynLogToBuffer(start_log_lsn_, txn_log_->txn_log_buffer_, log_size_);

        COMPILER_BARRIER
        log_buffer_->unpersistented_lsn_ += log_size_;
        log_buffer_->unreplicated_lsn_   += log_size_;

        log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

        // COMPILER_BARRIER
        // log_buffer_->filled_lsn_[txn_id_->thread_id_] = start_log_lsn_ + log_size_;
        // COMPILER_BARRIER
        // log_buffer_->filling_[txn_id_->thread_id_]    = false;
        
    #if   LOG_STRATEGY_TYPE == TAURUS_LOG
        /** 更新事务LSN向量 **/
        log_txn_meta_->txn_lsn_vector_[log_buf_id_] = start_log_lsn_ + log_size_;
    #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
        /** LogIndex日志策略，不以元组为单位进行日志commit，而是以事务为单位 **/
        log_strategy_->CommitTuple(this);
    #endif

        current_txn_->last_log_lsn_ = start_log_lsn_ + log_size_ - 1;


    #if PRE_COMMIT_TXN
        txn_thread_->pre_commit_queue_.push(current_txn_);
    #else
        //等待事务日志持久化完成
        while (current_txn_->last_log_lsn_ >= log_buffer_->persistented_lsn_)
        {
            PAUSE
        }
    #endif

#endif

        //事务提交，调用并发控制策略
        cc_strategy_->CommitTxn(this);        
    }
    else if (rc == RC::RC_ABORT)
    {
        cc_strategy_->AbortTxn(this);
    }
    
    cc_strategy_->FinishTxn(this, rc);

    return rc;
}


void TxnContext::ResetContext()
{
    /**** 事务 ****/
    txn_id_ = nullptr;
    current_txn_ = nullptr;
    
    /**** 并发控制 ****/
    next_access_entry_ = 0;
    cc_txn_meta_->Reset();

    /**** 日志 ****/
    txn_log_->log_offset_ = 0;
    start_log_lsn_        = 0;
    log_size_             = 0;
    log_txn_meta_->Reset();
}