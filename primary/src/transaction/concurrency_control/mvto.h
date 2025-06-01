#ifndef CC_MVTO_H_
#define CC_MVTO_H_


#include "config.h"

#include "cc_strategy.h"

#include "tuple.h"


#include <pthread.h>



class MvtoTxnMeta;
class TxnState;
class Tuple;
class LogEntry;

#if CC_STRATEGY_TYPE == MVTO_CC


class ReqEntry
{
public:

    ReqEntry(Tuple* tuple, AccessType access_type, Timestamp req_ts);
    ~ReqEntry();

    AccessType    access_type_;
    Timestamp     req_ts_;

    Tuple*        visible_version_;
    Tuple*        new_version_;

    volatile bool req_ready_;
    volatile bool req_abort_;

    ReqEntry*     next_;
};


class MvtoVersionMeta : public Tuple
{
private:

    void GetLatch();
    void ReleaseLatch();

    RC FindVisibleTuple(Timestamp ts, Tuple* &tuple);
    RC BufferRequest(ReqEntry* req_entry);
    RC RemoveRequest(ReqEntry* req_entry);


    // pthread_mutex_t* latch;
    volatile bool blatch_;

    Tuple*   latest_version_;
    uint32_t version_num_;
    
    volatile bool deleted_;

    Timestamp latest_wts_;
    Timestamp oldest_wts_;
    Timestamp max_rts_;

    bool      exist_prewrite_;
    ReqEntry* pre_write_req_;

    ReqEntry* requests_head_;


public:
    MvtoVersionMeta();
    MvtoVersionMeta(Tuple* tuple);
    ~MvtoVersionMeta();

    RC RequestAccess(ReqEntry* req_entry);
    RC FinishAccess(ReqEntry* req_entry, RC rc, Timestamp invisible_ts);
    
    void ClearInVisibleVersion(Timestamp invisible_ts);

    RC ReplayVersion(Tuple* tuple);

};


class MvtoAccess : public Access
{
public:

    ~MvtoAccess();
    
    MvtoVersionMeta* mvto_version_meta_;
    Tuple*           operate_version_;

    ReqEntry* req_entry;

};


class MvtoStrategy : public CCStrategy
{
private:

    volatile Timestamp latest_ts_;
    volatile Timestamp oldest_ts_;

    

public:

    MvtoStrategy();


    Timestamp GetTxnTimestamp();

    RC AccessTuple(TxnState* txn_state, AccessType access_type, TableID table_id, Tuple* origin_tuple, Tuple* &operate_tuple);

    RC BeginTxn(TxnState* txn_state);
    RC PrepareTxn(TxnState* txn_state);
    RC CommitTxn(TxnState* txn_state);
    RC AbortTxn(TxnState* txn_state);

    void FinishTxn(TxnState* txn_state, RC rc);

    void ReleaseTxnData(TxnState* txn_state, RC rc);

};



class MvtoTxnMeta : public CCTxnMeta
{
public:
    
    MvtoTxnMeta();
    ~MvtoTxnMeta();

    void Reset();

    Timestamp GetTimestamp();

    Timestamp timestamp_;

};



class MvtoTupleMeta : public CCTupleMeta
{
public:

    MvtoTupleMeta();

    Tuple* next_version_;
    uint64_t w_ts_;

};





#endif

#endif