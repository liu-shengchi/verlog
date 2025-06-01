#ifndef CC_SILO_H_
#define CC_SILO_H_


#include "config.h"

#include "access_entry.h"
#include "cc_strategy.h"



class SiloTxnMeta;
class TxnState;
class Tuple;


class SiloAccess : public AccessEntry
{
public:

};



class SiloStrategy : public CCStrategy
{
public:


    RC AccessTuple(TxnState* txn_state, AccessType access_type, TableID table_id, Tuple* origin_tuple, Tuple* &return_tuple);

    RC BeginTxn(TxnState* txn_state);
    RC PrepareTxn(TxnState* txn_state);
    RC CommitTxn(TxnState* txn_state);
    RC AbortTxn(TxnState* txn_state);

};



class SiloTxnMeta : public CCTxnMeta
{
public:


};



class SiloTupleMeta : public CCTupleMeta
{
public:


};




#endif