#ifndef APPLY_WRITES_ON_DEMAND_H_
#define APPLY_WRITES_ON_DEMAND_H_


#include "config.h"
#include "global.h"

#include "am_strategy.h"
#include "access_obj.h"

#include "db_latch.h"

#include "tuple.h"


#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

class WriteInfo
{
public:
    Timestamp     commit_ts_;
    LogLogicalPtr log_ptr_;
    LogSize       log_size_;

    DBLatch       lock_;  
    volatile bool is_applying_; 
    volatile bool is_applied_; 

    WriteInfo* next_write_info_;

    WriteInfo(Timestamp commit_ts, LogLogicalPtr log_ptr, LogSize log_size);
};


class WriteInfoAO : public AccessObj
{
public:
    WriteInfoAO();

    WriteInfo* latest_write_info_;

    void InsertNewWriteInfo(WriteInfo* write_info);
};



class ApplyWritesOnDemandAO : public AccessObj
{
public:

    ApplyWritesOnDemandAO();

    Tuple*   latest_version_;
    
    void InsertNewVersion(Tuple* new_tuple_ver);

};


class ApplyWritesOnDemandAM : public AMStrategy
{
private:
    
public:
    ApplyWritesOnDemandAM();
    ~ApplyWritesOnDemandAM();


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

    bool ReplayTupleLog(LogTupleMeta* log_tuple_meta, 
                        LogReplayContext* replay_context);

};



#endif
#endif