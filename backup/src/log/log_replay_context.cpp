#include "log_replay_context.h"

#include "replay_thread.h"

#include "am_strategy.h"

#include "log_buffer.h"

#include "serial_log.h"
#include "taurus_log.h"
#include "gsn_local_log.h"
#include "logindex_log.h"



/*********** logReplayContext **********/

LogReplayContext::LogReplayContext()
{
    replay_thread_ = nullptr;

    am_strategy_   = nullptr;

    log_strategy_  = nullptr;
    
    replay_log_buffer_ = nullptr;
    replay_log_buf_id_ = 0;

    replayed_lsn_        = 0;
    replayed_commit_ts_  = 0;
    replaying_commit_ts_ = 0;

    log_txn_meta_  = nullptr;
    log_tuple_num_ = 0;
    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
    {
        log_tuple_meta_[i] = nullptr;
    }    


    is_time_breakdown_sample_  = false;
    is_micro_statistic_sample_ = false;
}

LogReplayContext::~LogReplayContext()
{
    
}


void LogReplayContext::InitContext(ReplayThread* replay_thread, AMStrategy* am_strategy, LogStrategy* log_strategy, LogBuffer* log_buffer, uint64_t replay_thread_index)
{
    replay_thread_     = replay_thread;
    replay_thread_id_  = replay_thread_->replay_thread_id_;

    replay_log_buffer_ = log_buffer;
    replay_log_buf_id_ = log_buffer->log_buf_id_;
    replay_context_index_ = replay_thread_index;

    am_strategy_  = am_strategy;
    log_strategy_ = log_strategy;

#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    log_txn_meta_ = new SerialLogTxnMeta();
    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
        log_tuple_meta_[i] = new SerialLogTupleMeta();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    log_txn_meta_ = new TaurusLogTxnMeta();
    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
        log_tuple_meta_[i] = new TaurusLogTupleMeta();
#elif LOG_STRATEGY_TYPE == GSN_LOCAL_LOG
    log_txn_meta_ = new GSNLocalLogTxnMeta();
    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
        log_tuple_meta_[i] = new GSNLocalLogTupleMeta();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    log_txn_meta_ = new LogIndexTxnMeta();
    for (uint64_t i = 0; i < MAX_WRITE_ACCESS_PER_TXN; i++)
        log_tuple_meta_[i] = new LogIndexTupleMeta();
#endif

}


bool LogReplayContext::GetReplayTxnLog()
{
    return replay_log_buffer_->GetReplayTxnLog(this);
}


void LogReplayContext::DeconstructTxnLog()
{
    log_strategy_->DeconstructTxnLog(this);
}

void LogReplayContext::ReplayTxnLog()
{
    log_strategy_->ReplayTxnLog(this);
}


void LogReplayContext::FinishReplayTxn()
{
    //表示该回放线程在该日志缓冲区中的回放进度达到的commit_ts_和lsn_。
    if (unreplayed_log_tuple_.empty())
    {
        //如果不存在未回放日志，则说明该relayer已经完成当前事务日志以及之前日志的回放任务
        replayed_commit_ts_ = log_txn_meta_->commit_ts_;
        replayed_lsn_       = log_txn_meta_->start_lsn_ + log_txn_meta_->log_size_;
    }
    else
    {
        LogTupleMeta* log_tuple_meta = unreplayed_log_tuple_.front();
        replayed_commit_ts_ = log_tuple_meta->commit_ts_ - 1;
        replayed_lsn_       = log_tuple_meta->start_lsn_;
    }

    //需要重置、释放一些数据结构
    log_strategy_->FinishReplayTxn(this);
}




bool LogReplayContext::IsTimeBreakdownSample()
{
    return is_time_breakdown_sample_;
}

bool LogReplayContext::IsMicroStatisticSample()
{
    return is_micro_statistic_sample_;
}


void LogReplayContext::ResetContext()
{
    log_txn_meta_->Reset();
    log_tuple_num_ = 0;
}



ThreadID LogReplayContext::GetReplayThreadID()
{
    return replay_thread_id_;
}
