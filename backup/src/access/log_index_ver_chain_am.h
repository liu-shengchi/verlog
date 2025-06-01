#ifndef ACCESS_LOG_INDEX_VER_CHAIN_H_
#define ACCESS_LOG_INDEX_VER_CHAIN_H_

#include "config.h"
#include "access_obj.h"
#include "am_strategy.h"

#include "db_latch.h"

#if  AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM

class LogTxnMeta;
class LogTupleMeta;

class Tuple;


class LogIndexVerChainAO : public AccessObj
{
public:

    LogLogicalPtr log_logical_ptr_ = 0;
    MemPtr        log_mem_ptr_ = 0x0;

    DBLatch   data_ver_chain_latch_;

    Tuple*    data_ver_chain_ = nullptr;
    Timestamp data_ver_chain_max_ts_ = 0;


    Timestamp created_ts_ = 0;
};


class LogIndexVerChainAM : public AMStrategy
{
private:
    
public:
    LogIndexVerChainAM();
    ~LogIndexVerChainAM();

    bool GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      Timestamp visible_ts, 
                      ThreadID txn_thread_id = 0);

    bool GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      TxnContext* txn_context,
                      ShardID    shard_id = 0,
                      PrimaryKey primary_key = 0);

    bool ReplayTupleLog(LogTupleMeta* log_tuple_meta, 
                        LogReplayContext* replay_context);

};




#endif
#endif