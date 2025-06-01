#ifndef CC_STRATEGY_H_
#define CC_STRATEGY_H_

#include "config.h"


class TxnContext;
class TxnState;
class Tuple;
class AccessEntry;



class CCTxnMeta
{
public:

    virtual void Reset() = 0;
    
};


class CCTupleMeta
{
public:

    virtual void CopyCCTupleMeta(CCTupleMeta* cc_tuple_meta) = 0;
    virtual void Reset(){};
};



class CCStrategy
{
public:
    
    virtual RC AccessTuple(CCTxnMeta* cc_txn_meta, AccessEntry* access, 
                           AccessType access_type, TableID table_id, ShardID shard_id, 
                           Tuple* origin_tuple, Tuple* &operate_tuple) = 0;
    
    virtual RC BeginTxn(TxnContext* txn_context) = 0;
    virtual RC PrepareTxn(TxnContext* txn_context) = 0;
    
    virtual RC CommitTxn(TxnContext* txn_context) = 0;
    virtual RC AbortTxn(TxnContext* txn_context) = 0;

    //finish txn, release data structures that relate to this txn.
    virtual void FinishTxn(TxnContext* txn_context, RC rc) = 0;

    virtual void ReleaseTxnData(TxnContext* txn_context, RC rc) = 0;

};




#endif