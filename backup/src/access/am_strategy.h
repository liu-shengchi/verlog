#ifndef ACCESS_ACCESS_METHOD_H_
#define ACCESS_ACCESS_METHOD_H_

#include "config.h"


class AccessObj;

class LogTxnMeta;
class LogTupleMeta;

class TxnContext;

class LogReplayContext;

class AMStrategy
{
private:
    
public:

    virtual bool GetTupleData(AccessObj* access_obj, 
                              TableID table_id, 
                              TupleData tuple_data, 
                              Timestamp visible_ts, 
                              ThreadID txn_thread_id = 0) = 0;
    
    virtual bool GetTupleData(AccessObj* access_obj, 
                              TableID table_id, 
                              TupleData tuple_data, 
                              TxnContext* txn_context,
                              ShardID    shard_id = 0,
                              PrimaryKey primary_key = 0) = 0;

    virtual bool ReplayTupleLog(LogTupleMeta*     log_tuple_meta, 
                                LogReplayContext* replay_context) = 0;

};


#endif