#ifndef ACCESS_QUERYFRESH_AM_H_
#define ACCESS_QUERYFRESH_AM_H_

#include "config.h"
#include "access_obj.h"
#include "am_strategy.h"
#include "db_spin_lock.h"

#if  AM_STRATEGY_TYPE == QUERY_FRESH_AM


class LogTxnMeta;
class LogTupleMeta;

class Tuple;


class ReplayArrayAO : public AccessObj
{
public:
    volatile LogLogicalPtr log_logical_addr_ = 0;
    volatile Timestamp     commit_ts_        = 0;
};


class QueryFreshAO : public AccessObj
{
public:
    QueryFreshAO();

    Tuple*         data_ver_chain_   = nullptr;
    LogLogicalPtr  log_logical_addr_ = 0;
};



class QueryFreshAM : public AMStrategy
{
private:
    
public:
    QueryFreshAM();
    ~QueryFreshAM();

    bool GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      Timestamp visible_ts, 
                      ThreadID txn_thread_id = 0) {return true;};


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