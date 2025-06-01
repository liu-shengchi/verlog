#ifndef ACCESS_TUPLE_VER_CHAIN_AM_H_
#define ACCESS_TUPLE_VER_CHAIN_AM_H_

#include "config.h"

#include "am_strategy.h"
#include "access_obj.h"


#if  AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM

class Tuple;
class TxnContext;
class LogTxnMeta;
class LogTupleMeta;


class TupleVerChainAO : public AccessObj
{
public:

    TupleVerChainAO();

    Tuple*   latest_version_;
    
    void InsertNewVersion(Tuple* new_tuple_ver, bool deleted);

};

 
class TupleVerChainAM : public AMStrategy
{
private:
    
public:
    TupleVerChainAM();
    ~TupleVerChainAM();


    bool GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      Timestamp visible_ts, 
                      ThreadID replay_thread_id = 0);

    bool GetTupleData(AccessObj* access_obj, 
                      TableID table_id, 
                      TupleData tuple_data, 
                      TxnContext* txn_context,
                      ShardID    shard_id = 0,
                      PrimaryKey primary_key = 0);

    bool ReplayTupleLog(LogTupleMeta* log_tuple_meta, LogReplayContext* replay_context);

};

#endif
#endif