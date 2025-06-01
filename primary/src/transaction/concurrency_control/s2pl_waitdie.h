#ifndef CC_S2PL_WAIEDIE_H_
#define CC_S2PL_WAIEDIE_H_


#include "config.h"

#include "access_entry.h"

#include "cc_strategy.h"

#include "db_latch.h"


class S2plWDTxnMeta;
class TxnContext;
class Tuple;

#if CC_STRATEGY_TYPE == S2PL_WAITDIE_CC


enum LockType 
{
    LOCK_SH,
    LOCK_EX,
    LOCK_NONE
};

enum LockState
{
    LOCK_WAIT,
    LOCK_OK,
    LOCK_ABORT
};


class LockEntry {
public:
    LockType        lock_type;
    S2plWDTxnMeta*  cc_txn_meta;
	LockEntry*      next;
	LockEntry*      prev;
};



class S2plWDAccess : public AccessEntry
{
public:

    S2plWDAccess();
    ~S2plWDAccess();

    //在进行更新操作前，复制origin_tuple_
    //由于2PL允许事务直接对元组进行修改，需要将修改前的数据状态复制到本地
    //如果事务回滚，可以将元组数据恢复到访问前的状态
    Tuple*      copy_tuple_;

    /*
     * 事务优先级
     * 在目前的设计下，等于事务的开始时间戳
     * 时间戳越小，事务优先级越高
     */
    uint64_t priority_;

    //锁机制
    LockType           lock_type_;
    volatile LockState lock_state_;

    /*
     * 用于构造元组锁链表
     * 事务的Access数据结构，代表“锁”结构，元组的锁列表和等待列表，
     * 由不同事务的Access连接而成。
     */
    S2plWDAccess* next_;
    
};


class S2plWDStrategy : public CCStrategy
{
private:

    uint64_t volatile begin_ts_;

public:
    S2plWDStrategy();
    ~S2plWDStrategy();

    RC AccessTuple(CCTxnMeta* cc_txn_meta, AccessEntry* access, AccessType access_type, TableID table_id, ShardID shard_id, Tuple* origin_tuple, Tuple* &operate_tuple);
    
    RC BeginTxn(TxnContext* txn_context);
    RC PrepareTxn(TxnContext* txn_context);
    RC CommitTxn(TxnContext* txn_context);
    RC AbortTxn(TxnContext* txn_context);

    void FinishTxn(TxnContext* txn_context, RC rc);
    void ReleaseTxnData(TxnContext* txn_context, RC rc);

};


class S2plWDTxnMeta : public CCTxnMeta
{
public:
    void Reset();

    uint64_t GetPriority();
    
    //txn priority
    uint64_t timestamp_;

};


class S2plWDTupleMeta : public CCTupleMeta
{
public:
    S2plWDTupleMeta();
    ~S2plWDTupleMeta();

    RC GetLock(S2plWDAccess* access);
    RC ReleaseLock(S2plWDAccess* access);
    
    // RC GetLock(LockType type, S2plWDTxnMeta * cc_txn_meta);
    // // RC lock_get(LockType type, txn_man * txn, uint64_t* &txnids, uint64_t &txncnt);
    // RC ReleaseLock(S2plWDTxnMeta * cc_txn_meta);

    //1. txn aborts, the tuple that the txn inserted need to delete, 
    //   these txns that are waiting the txn commits need to abort.
    //2. txn commits, the tuple that the txn logically deleted need to delete physiscally,
    //   these txns that are waiting the txn aborts need to abort.
    RC AbortWaitingTxn();

    void CopyCCTupleMeta(CCTupleMeta* cc_tuple_meta);


private:
    bool 		ConflictLock(LockType l1, LockType l2);

    /**
     * protect concurrent data structure below
     * At the same time, only one thread is allowed to access the data structure below
     * **/
    DBLatch db_latch_;

    LockType      lock_type_;
    
    // owners and waiters are both two single linked list.
	// [waiters] head is the youngest txn, tail is the oldest txn.
    S2plWDAccess* owners_;
    S2plWDAccess* waiters_;

};



#endif
#endif