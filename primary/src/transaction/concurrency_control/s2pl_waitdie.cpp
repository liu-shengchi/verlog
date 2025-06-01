#include "s2pl_waitdie.h"

#include "global.h"

#include "tpcc_schema.h"


#include "txn_context.h"

#include "log_strategy.h"
#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"


#include "index.h"
#include "tuple.h"

#include <stdio.h>
#include <typeinfo>
#include <pthread.h>
#include <malloc.h>
#include <vector>


using namespace std;


#if CC_STRATEGY_TYPE == S2PL_WAITDIE_CC

/*******************************************/
/*************** S2plWDAccess **************/
/*******************************************/

S2plWDAccess::S2plWDAccess()
{
    copy_tuple_ = new Tuple(MAX_TUPLE_SIZE);
}


S2plWDAccess::~S2plWDAccess()
{
    delete copy_tuple_;
}



/*******************************************/
/************** S2plWDStrategy *************/
/*******************************************/

S2plWDStrategy::S2plWDStrategy()
{
    
}

S2plWDStrategy::~S2plWDStrategy()
{

}

RC S2plWDStrategy::AccessTuple(CCTxnMeta* cc_txn_meta, AccessEntry* access, AccessType access_type, TableID table_id, 
                                ShardID shard_id, Tuple* origin_tuple, Tuple* &operate_tuple)
{
    RC rc = RC::RC_OK;

    /*****  *****/
    S2plWDTxnMeta*   s2plwd_txn_meta   = dynamic_cast<S2plWDTxnMeta*>(cc_txn_meta);
    S2plWDTupleMeta* s2plwd_tuple_meta = origin_tuple->cc_tuple_meta_;
    S2plWDAccess*    s2plwd_access     = dynamic_cast<S2plWDAccess*>(access);

    if (access_type == AccessType::READ_AT)
        s2plwd_access->lock_type_ = LockType::LOCK_SH;
    else
        s2plwd_access->lock_type_ = LockType::LOCK_EX;

    s2plwd_access->priority_    = s2plwd_txn_meta->GetPriority();
    s2plwd_access->lock_state_  = LOCK_WAIT;
    s2plwd_access->next_        = nullptr;

    rc = s2plwd_tuple_meta->GetLock(s2plwd_access);

    if (rc == RC_OK) { ; }
    else if (rc == RC_ABORT) { ; }
    else if (rc == RC_WAIT)
    {
        //wait for transaction get lock or abort
        while (s2plwd_access->lock_state_ == LOCK_WAIT)
            continue;

        if (s2plwd_access->lock_state_ == LOCK_OK){
            rc = RC_OK;
        }
        else if (s2plwd_access->lock_state_ == LOCK_ABORT){
            rc = RC_ABORT;
        }
    }

    if (rc == RC_ABORT) { return rc; }

    s2plwd_access->access_type_   = access_type;
    s2plwd_access->table_id_      = table_id;
    s2plwd_access->shard_id_      = shard_id;
    s2plwd_access->origin_tuple_  = origin_tuple;
    s2plwd_access->operate_tuple_ = origin_tuple;

    if (access_type == AccessType::READ_AT)
    {
        ;
    }
    else if (access_type == AccessType::UPDATE_AT)
    {
		s2plwd_access->copy_tuple_->CopyTupleData(origin_tuple, g_schema->GetTupleSize(table_id));
    }
    else if (access_type == AccessType::DELETE_AT)
    {
        ;
    }
    else if (access_type == AccessType::INSERT_AT)
    {
        ;
    }
    
    operate_tuple = access->operate_tuple_;
    
    return rc;
}


RC S2plWDStrategy::BeginTxn(TxnContext* txn_context)
{
    RC rc = RC::RC_OK;

    //initial txn meta data
    S2plWDTxnMeta* cc_txn_meta = txn_context->cc_txn_meta_;
    
    GET_CLOCK_TIME(cc_txn_meta->timestamp_);
    
    return rc;
}


RC S2plWDStrategy::PrepareTxn(TxnContext* txn_context)
{
    RC rc = RC_COMMIT;
    
    return rc;
}


RC S2plWDStrategy::CommitTxn(TxnContext* txn_context)
{
    RC rc = RC::RC_OK;
    
    for (uint64_t  i = 0; i < txn_context->next_access_entry_; i++)
    {
        S2plWDAccess*    access        = dynamic_cast<S2plWDAccess*>(txn_context->txn_access_entry_[i]);
        Tuple*           origin_tuple  = access->origin_tuple_;
        S2plWDTxnMeta*   cc_txn_meta   = txn_context->cc_txn_meta_;
        S2plWDTupleMeta* cc_tuple_meta = origin_tuple->cc_tuple_meta_;

        if (access->access_type_ == AccessType::DELETE_AT)
        {
            Index*   index;
            IndexKey index_key;

            //the order of primary key index is same as the order of table
            index     = g_schema->GetIndex(access->table_id_, access->shard_id_);
            index_key = g_schema->GetIndexKey(access->table_id_, origin_tuple->tuple_data_);
            index->IndexRemove(index_key);
            
            cc_tuple_meta->AbortWaitingTxn();
        }

        /* 
         * LogIndex策略在TxnContext::FinishTxn阶段已经执行CommitTuple。这里无需执行
         */
#if !NO_LOG && LOG_STRATEGY_TYPE != LOGINDEX_LOG
        /** 调用日志提交元组函数，修改对应日志元数据 **/
        txn_context->log_strategy_->CommitTuple(txn_context, access);
#endif

        cc_tuple_meta->ReleaseLock(access);
    }

    return rc;
}



RC S2plWDStrategy::AbortTxn(TxnContext* txn_context)
{
    RC rc = RC::RC_OK;

    for (uint64_t  i = 0; i < txn_context->next_access_entry_; i++)
    {
        S2plWDAccess*    access        = dynamic_cast<S2plWDAccess*>(txn_context->txn_access_entry_[i]);
        Tuple*           origin_tuple  = access->origin_tuple_;
        S2plWDTupleMeta* cc_tuple_meta = origin_tuple->cc_tuple_meta_;
        
        if (access->access_type_ == AccessType::UPDATE_AT)
        {
            origin_tuple->CopyTupleData(access->copy_tuple_, g_schema->GetTupleSize(access->table_id_));
        }
        else if (access->access_type_ == AccessType::INSERT_AT)
        {
            //remove insert tuple from index
            //maybe, other txns wait for current txn commit and release lock
            //abort these txns
            Index*   index;
            IndexKey index_key;

            //the order of primary key index is same as the order of table
            index     = g_schema->GetIndex(access->table_id_, access->shard_id_);
            index_key = g_schema->GetIndexKey(access->table_id_, origin_tuple->tuple_data_);
            index->IndexRemove(index_key);
            
            cc_tuple_meta->AbortWaitingTxn();
        }

        cc_tuple_meta->ReleaseLock(access);
    }


    return rc;
}

void S2plWDStrategy::FinishTxn(TxnContext* txn_context, RC rc)
{

}

void S2plWDStrategy::ReleaseTxnData(TxnContext* txn_context, RC rc)
{

}



/*******************************************/
/************** S2plWDTxnMeta **************/
/*******************************************/

void S2plWDTxnMeta::Reset()
{
    timestamp_ = 0;
}


uint64_t S2plWDTxnMeta::GetPriority()
{
    return timestamp_;
}




/*******************************************/
/************* S2plWDTupleMeta *************/
/*******************************************/

S2plWDTupleMeta::S2plWDTupleMeta()
{
	// latch_  = new pthread_mutex_t;
    // blatch_ = false;
	// pthread_mutex_init(latch_, NULL);
	
    lock_type_ = LOCK_NONE;
    
	owners_    = nullptr;
	waiters_   = nullptr;

    // owner_cnt_    = 0;
	// waiter_cnt_   = 0;

	// owners_       = nullptr;
	// waiters_head_ = nullptr;
	// waiters_tail_ = nullptr;


	// blatch_ = false;
}


S2plWDTupleMeta::~S2plWDTupleMeta()
{
    // delete latch_;
}


RC S2plWDTupleMeta::GetLock(S2plWDAccess* access)
{
	RC rc = RC::RC_OK;

    db_latch_.GetLatch();

    bool conflict = ConflictLock(lock_type_, access->lock_type_);
    
    if (!conflict)
    {
        if (waiters_ != nullptr && access->priority_ < waiters_->priority_)
			conflict = true;
    }

    if (conflict)
    {
        ///////////////////////////////////////////////////////////
        //  - T is the txn currently running
        //	IF T.ts < ts of all owners
        //		T can wait
        //  ELSE
        //      T should abort
        //////////////////////////////////////////////////////////
        bool canwait = true;

        S2plWDAccess* cur_access = owners_;
        S2plWDAccess* pre_access = nullptr;

        while (cur_access != nullptr) {
            if (cur_access->priority_ <= access->priority_) {
                canwait = false;
                break;
            }
            cur_access = cur_access->next_;
        }

        if (canwait) {
            // insert txn to the right position
            // the waiter list is always in reversed timestamp order
            cur_access = waiters_;
            while (cur_access != nullptr && access->priority_ < cur_access->priority_) {
                pre_access = cur_access;
                cur_access = cur_access->next_;
            }

            if (cur_access != nullptr) {
                
                access->next_ = cur_access;

                if (cur_access == waiters_) {
                    waiters_ = access;
                }
                else {
                    pre_access->next_ = access;
                }

            } else {

                if (waiters_ == nullptr) {
                    waiters_ = access;
                }
                else {
                    pre_access->next_ = access;
                }
                
            }
            
            access->lock_state_ = LOCK_WAIT;
            rc = RC_WAIT;
        }
        else {
            access->lock_state_ = LOCK_ABORT;
            rc = RC_ABORT;
        }
    }
    else {

		access->next_ = owners_;
        owners_       = access;
		
        lock_type_ = access->lock_type_;

        access->lock_state_ = LOCK_OK;
        rc = RC_OK;
	}

    db_latch_.ReleaseLatch();

    return rc;

}


RC S2plWDTupleMeta::ReleaseLock(S2plWDAccess* access)
{

	db_latch_.GetLatch();

	// Try to find the entry in the owners
	S2plWDAccess* cur  = owners_;
	S2plWDAccess* prev = nullptr;

	while (cur != nullptr && cur != access) {
		prev = cur;
		cur = cur->next_;
	}

	if (cur != nullptr) { // find the entry in the owner list
		if (prev == nullptr)  //cur == owners_ 
            { owners_ = cur->next_; }
		else 
            { prev->next_ = cur->next_; }

		if (owners_ == nullptr)
			{ lock_type_ = LOCK_NONE; }
	
    } else {
		// Not in owners list, try waiters list.
		cur  = waiters_;
        prev = nullptr;
		while (cur != nullptr && cur != access) { 
            prev = cur;
            cur  = cur->next_; 
        }

        //exceptional situation
        if (cur == nullptr) {
            db_latch_.ReleaseLatch();
            return RC_NULL;
        }


        if (prev == nullptr)
            { waiters_ = cur->next_; }
        else
            { prev->next_ = cur->next_; }
        
	}

	// If any waiter can join the owners, just do it!
	while (waiters_ && !ConflictLock(lock_type_, waiters_->lock_type_)) {
        //remover the head of waiters_
        //insert the position that the head of owners_
        S2plWDAccess* temp;
        temp = waiters_;
        waiters_ = waiters_->next_;
        temp->next_ = owners_;
        owners_ = temp;

		temp->lock_state_ = LOCK_OK;
		lock_type_ = temp->lock_type_;
	} 

	db_latch_.ReleaseLatch();

	return RC_OK;

}


RC S2plWDTupleMeta::AbortWaitingTxn()
{
    while (waiters_ != nullptr)
    {
        waiters_->lock_state_ = LOCK_ABORT;
        waiters_ = waiters_->next_;
    }
    
    return RC_OK;
}


void S2plWDTupleMeta::CopyCCTupleMeta(CCTupleMeta* cc_tuple_meta)
{
    S2plWDTupleMeta* source_cc_tuple_meta = dynamic_cast<S2plWDTupleMeta*>(cc_tuple_meta);

    lock_type_ = source_cc_tuple_meta->lock_type_;
    owners_    = source_cc_tuple_meta->owners_;
    waiters_   = source_cc_tuple_meta->waiters_;
}


// RC S2plWDTupleMeta::GetLock(LockType type, S2plWDTxnMeta * cc_txn_meta) {

// 	RC rc = RC::RC_OK;

//     // printf("try lock tuple!\n");
//     GetLatch();
//     // printf("lock tuple success!\n");

//     bool conflict = ConflictLock(lock_type_, type);

//     if (!conflict)
//     {
//         if (waiters_head_ != nullptr && cc_txn_meta->GetPriority() < waiters_head_->cc_txn_meta->GetPriority())
// 			conflict = true;
//     }

//     if (conflict)
//     {
//         ///////////////////////////////////////////////////////////
//         //  - T is the txn currently running
//         //	IF T.ts < ts of all owners
//         //		T can wait
//         //  ELSE
//         //      T should abort
//         //////////////////////////////////////////////////////////
//         bool canwait = true;

//         LockEntry * en = owners_;
//         while (en != nullptr) {
//             if (en->cc_txn_meta->GetPriority() < cc_txn_meta->GetPriority()) {
//                 canwait = false;
//                 break;
//             }
//             en = en->next;
//         }

//         if (canwait) {
//             // insert txn to the right position
//             // the waiter list is always in timestamp order
//             LockEntry * entry = get_entry();
//             entry->cc_txn_meta = cc_txn_meta;
//             entry->lock_type = type;
//             en = waiters_head_;
//             while (en != NULL && cc_txn_meta->GetPriority() < en->cc_txn_meta->GetPriority()) 
//                 en = en->next;

//             if (en != nullptr) {
//                 LIST_INSERT_BEFORE(en, entry);
//                 if (en == waiters_head_)
//                     waiters_head_ = entry;
//             } else {
//                 LIST_PUT_TAIL(waiters_head_, waiters_tail_, entry);
//             }
            
//             waiter_cnt_++;
//             cc_txn_meta->lock_ready_ = false;
//             rc = RC_WAIT;
//         }
//         else {
//             rc = RC_ABORT;
//             // printf("This txn need to abort! \n");
//         }
//     }
//     else {
// 		LockEntry * entry = get_entry();
// 		entry->lock_type = type;
// 		entry->cc_txn_meta = cc_txn_meta;
// 		STACK_PUSH(owners_, entry);
// 		owner_cnt_++;
// 		lock_type_ = type;
//         rc = RC_OK;
// 	}

//     ReleaseLatch();

//     return rc;
// }



// RC S2plWDTupleMeta::ReleaseLock(S2plWDTxnMeta * cc_txn_meta) {	

// 	GetLatch();

// 	// Try to find the entry in the owners
// 	LockEntry * en = owners_;
// 	LockEntry * prev = nullptr;

// 	while (en != nullptr && en->cc_txn_meta != cc_txn_meta) {
// 		prev = en;
// 		en = en->next;
// 	}

// 	if (en != nullptr) { // find the entry in the owner list
// 		if (prev == nullptr) 
//             { owners_ = en->next; }
// 		else 
//             { prev->next = en->next; }

// 		return_entry(en);
// 		owner_cnt_--;

// 		if (owner_cnt_ == 0)
// 			{ lock_type_ = LOCK_NONE; }
	
//     } else {
// 		// Not in owners list, try waiters list.
// 		en = waiters_head_;
// 		while (en != nullptr && en->cc_txn_meta != cc_txn_meta)
// 			{ en = en->next; }

// 		// ASSERT(en);
		
//         LIST_REMOVE(en);
		
//         if (en == waiters_head_)
// 			waiters_head_ = en->next;
// 		if (en == waiters_tail_)
// 			waiters_tail_ = en->prev;
		
//         return_entry(en);
// 		waiter_cnt_--;
// 	}

// // 	if (owner_cnt_ == 0)
// // 		ASSERT(lock_type == LOCK_NONE);
// // #if DEBUG_ASSERT && CC_ALG == WAIT_DIE 
// // 		for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
// // 			assert(en->next->txn->get_ts() < en->txn->get_ts());
// // #endif

// 	LockEntry * entry;
// 	// If any waiter can join the owners, just do it!
// 	while (waiters_head_ && !ConflictLock(lock_type_, waiters_head_->lock_type)) {
// 		LIST_GET_HEAD(waiters_head_, waiters_tail_, entry);
// 		STACK_PUSH(owners_, entry);
// 		owner_cnt_++;
// 		waiter_cnt_--;
// 		// ASSERT(entry->txn->lock_ready == false);
// 		entry->cc_txn_meta->lock_ready_ = true;
// 		lock_type_ = entry->lock_type;
// 	} 
// 	// ASSERT((owners == NULL) == (owner_cnt == 0));

// 	ReleaseLatch();

// 	return RC_OK;
// }


// RC S2plWDTupleMeta::AbortWaitingTxn()
// {
//     LockEntry* entry = waiters_head_;
//     LockEntry* prev_entry;
//     while (entry)
//     {
//         entry->cc_txn_meta->lock_abort_ = true;
//         prev_entry = entry;
//         entry = entry->next;
//         return_entry(prev_entry);
//     }
    
//     return RC_OK;
// }




bool S2plWDTupleMeta::ConflictLock(LockType l1, LockType l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
	else
		return false;
}

// LockEntry* S2plWDTupleMeta::get_entry() {
// 	LockEntry * entry = new LockEntry();
// 	return entry;
// }

// void S2plWDTupleMeta::return_entry(LockEntry * entry) {
// 	delete entry;
// }


// void S2plWDTupleMeta::GetLatch()
// {
//     while (!ATOM_CAS(blatch_, false, true)){ }
//     // pthread_mutex_lock(latch_);
// }

// void S2plWDTupleMeta::ReleaseLatch()
// {
//     ATOM_CAS(blatch_, true, false);
//     // pthread_mutex_unlock(latch_);

// }


#endif