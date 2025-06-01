#ifndef  LOG_REPLAY_CONTEXT_H_

#include "config.h"

#include <queue>


class ReplayThread;

class AMStrategy;

class LogBuffer;

class LogStrategy;
class LogTxnMeta;
class LogTupleMeta;

class LogIndexTupleMeta;
class SerialLogTupleMeta;

class LogReplayContext
{
private:
    
    ReplayThread* replay_thread_;
    ThreadID      replay_thread_id_;

    AMStrategy*   am_strategy_;

    LogStrategy*  log_strategy_;

    LogBufID      replay_log_buf_id_;
    LogBuffer*    replay_log_buffer_;
    uint64_t      replay_context_index_;
    
    volatile LogLSN    replayed_lsn_;
    volatile Timestamp replayed_commit_ts_;
    volatile Timestamp replaying_commit_ts_;

    LogTxnMeta*   log_txn_meta_;
    uint64_t      log_tuple_num_;
    LogTupleMeta* log_tuple_meta_[MAX_WRITE_ACCESS_PER_TXN];
    
#if   LOG_STRATEGY_TYPE == LOGINDEX_LOG
    std::queue<LogIndexTupleMeta*> unreplayed_log_tuple_;
#elif LOG_STRATEGY_TYPE == SERIAL_LOG
    std::queue<SerialLogTupleMeta*> unreplayed_log_tuple_;
#endif


    // 是否对timebreak分析进行抽样
    bool   is_time_breakdown_sample_;    

    bool   is_micro_statistic_sample_;


    /*************** 友元类 ***************/
    friend class LogBuffer;
    
    friend class ReplayThread;

    friend class SerialLog;
    friend class TaurusLog;
    friend class GSNLocalLog;
    friend class LogIndexLog;


public:
    LogReplayContext();
    ~LogReplayContext();

    void InitContext(ReplayThread* replay_thread, 
                     AMStrategy* am_strategy, 
                     LogStrategy* log_strategy, 
                     LogBuffer* log_buffer, 
                     uint64_t replay_thread_index);

    bool GetReplayTxnLog();

    void DeconstructTxnLog();

    void ReplayTxnLog();

    void FinishReplayTxn();

    bool IsTimeBreakdownSample();

    bool IsMicroStatisticSample();

    void ResetContext();

    ThreadID GetReplayThreadID(); 

};



#endif