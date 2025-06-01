#include "mvto.h"

#include "global.h"

#include "tpcc_schema.h"

#include "index.h"

#include "txn_id.h"
#include "txn_context.h"
#include "tuple.h"

#include <stdio.h>


#if CC_STRATEGY_TYPE == MVTO_CC



/*********** MvtoStrategy ************/

MvtoStrategy::MvtoStrategy()
{
    latest_ts_ = 0;
    oldest_ts_ = 0;
}


Timestamp MvtoStrategy::GetTxnTimestamp()
{
    return GetServerClock();
}

RC MvtoStrategy::AccessTuple(TxnState* txn_state, AccessType access_type, TableID table_id, Tuple* origin_tuple, Tuple* &operate_tuple)
{
    RC rc = RC::RC_OK;

    MvtoTxnMeta*     cc_txn_meta       = txn_state->cc_txn_meta_;
    MvtoVersionMeta* mvto_version_meta = nullptr;
    Tuple*           tuple             = nullptr;

    if (access_type == INSERT_AT)
    {
        mvto_version_meta = new MvtoVersionMeta();
        tuple = origin_tuple;
    }
    else
    {
        mvto_version_meta = (MvtoVersionMeta*)origin_tuple;
        tuple = nullptr;
    }

    ReqEntry* req_entry = new ReqEntry(tuple, access_type, cc_txn_meta->GetTimestamp());
    
    rc = mvto_version_meta->RequestAccess(req_entry);

    if (rc == RC_OK)
        ;
    else if (rc == RC_WAIT)
    {
        while (!req_entry->req_ready_ && !req_entry->req_abort_){}

        if (req_entry->req_abort_)
            rc = RC_ABORT;
        else if (req_entry->req_ready_)
            rc = RC_OK;
    }

    if (rc == RC_ABORT)
    {
        delete req_entry;
        return rc;
    }

    MvtoAccess* access = new MvtoAccess();
    txn_state->accesses_.push_back(access);

    access->access_type_ = access_type;
    access->table_id_    = table_id;
    access->shard_id_    = 0;
    access->req_entry    = req_entry;

    if (access_type == READ_AT)
    {
        access->mvto_version_meta_ = mvto_version_meta;
        access->operate_version_   = req_entry->visible_version_;

        operate_tuple = access->operate_version_;
    }
    else if (access_type == UPDATE_AT)
    {
        access->mvto_version_meta_ = mvto_version_meta;

        Tuple* new_version = new Tuple(g_schema->GetTupleSize(table_id));
        new_version->CopyTupleData(req_entry->visible_version_, g_schema->GetTupleSize(table_id));
        new_version->cc_tuple_meta_->next_version_ = req_entry->visible_version_;
        new_version->cc_tuple_meta_->w_ts_         = req_entry->req_ts_;

        req_entry->new_version_  = new_version;
        access->operate_version_ = new_version;
        
        operate_tuple = new_version;
    }
    else if (access_type == INSERT_AT)
    {   
        access->mvto_version_meta_ = mvto_version_meta;

        access->operate_version_ = req_entry->new_version_;

        access->operate_version_->cc_tuple_meta_->next_version_ = nullptr;
        access->operate_version_->cc_tuple_meta_->w_ts_        = req_entry->req_ts_;

        operate_tuple = (Tuple*)mvto_version_meta;
    }
    else if (access_type == DELETE_AT)
    {
        access->mvto_version_meta_ = mvto_version_meta;
        access->operate_version_ = nullptr;

        operate_tuple = access->operate_version_;
    }
    
    return rc;
}


RC MvtoStrategy::BeginTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    txn_state->cc_txn_meta_->timestamp_ = GetTxnTimestamp();

    // printf("timestamp: %ld \n", txn_state->cc_txn_meta_->timestamp_);

    return rc;
}


RC MvtoStrategy::PrepareTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    return rc;
}


RC MvtoStrategy::CommitTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    for (vector<MvtoAccess*>::iterator ite = txn_state->accesses_.begin(); ite != txn_state->accesses_.end(); ite++)
    {
        MvtoAccess*      access   = *ite;

        MvtoVersionMeta* mv_meta  = access->mvto_version_meta_;
        ReqEntry*        req_entry = access->req_entry;
        AccessType       access_type = access->access_type_;

        if (access_type == READ_AT)
        {
            ;
        }
        else
        {
            if (access_type == DELETE_AT)
            {
                Index*   index;
                IndexKey index_key;

                //the order of primary key index is same as the order of table
                index     = g_schema->GetIndex(access->table_id_, access->shard_id_);
                index_key = g_schema->GetIndexKey(access->table_id_, req_entry->visible_version_->tuple_data_);
                index->IndexRemove(index_key);
            }
            
            // printf("oledst ts is: %ld \n", oldest_ts_);

            mv_meta->FinishAccess(req_entry, RC_COMMIT, oldest_ts_);
        }
    }

    return rc;
}


RC MvtoStrategy::AbortTxn(TxnState* txn_state)
{
    RC rc = RC::RC_OK;

    for (vector<MvtoAccess*>::iterator ite = txn_state->accesses_.begin(); ite != txn_state->accesses_.end(); ite++)
    {
        MvtoAccess*      access   = *ite;
        
        MvtoVersionMeta* mv_meta  = access->mvto_version_meta_;
        ReqEntry*        req_entry = access->req_entry;
        AccessType       access_type = access->access_type_;

        if (access_type == READ_AT)
        {
            ;
        }
        else
        {
            if (access_type == INSERT_AT)
            {
                //the order of primary key index is same as the order of table
                Index*   index     = g_schema->GetIndex(access->table_id_, access->shard_id_);
                IndexKey index_key = g_schema->GetIndexKey(access->table_id_, req_entry->new_version_->tuple_data_);
                index->IndexRemove(index_key);
            }
            
            mv_meta->FinishAccess(req_entry, RC_ABORT, oldest_ts_);
        }
    }

    return rc;
}


void MvtoStrategy::FinishTxn(TxnState* txn_state, RC rc)
{

    Timestamp ts = txn_state->cc_txn_meta_->GetTimestamp();

    TxnState** txn_states = g_txn_manager->GetAllTxnStates();

    Timestamp cur_oldest_ts = ts;
    for (uint64_t i = 0; i < g_txn_thread_num; i++)
    {
        if (cur_oldest_ts > txn_states[i]->cc_txn_meta_->GetTimestamp())
        {
            cur_oldest_ts = txn_states[i]->cc_txn_meta_->GetTimestamp();
            break;
        }
    }

    // printf("cur txn ts is: %ld\n", ts);
    // printf("cur oldest ts is: %ld\n", cur_oldest_ts);

    if (ts == cur_oldest_ts)
    {
        // printf("cur txn is oldest!\n");

        oldest_ts_ = ts;
    }

    for (vector<MvtoAccess*>::iterator ite = txn_state->accesses_.begin(); ite != txn_state->accesses_.end(); ite++)
    {
        MvtoAccess* access = *ite;
        
        if (access->access_type_ == READ_AT)
        {
            ;
        }
        else if (access->access_type_ == UPDATE_AT)
        {
            if (rc == RC_ABORT)
            {
                delete access->req_entry->new_version_;
            }
        }
        else if (access->access_type_ == INSERT_AT)
        {
            if (rc == RC_ABORT)
            {
                delete access->mvto_version_meta_;
                delete access->operate_version_;
            }
            
        }
        else if (access->access_type_ == DELETE_AT)
        {
            
        }
        
        delete access->req_entry;
    }

}


void MvtoStrategy::ReleaseTxnData(TxnState* txn_state, RC rc)
{
    if (rc == RC_COMMIT)
    {
        for (vector<MvtoAccess*>::iterator ite = txn_state->accesses_.begin(); ite != txn_state->accesses_.end(); ite++)
        {
            MvtoAccess* access = *ite;
            
            delete access->req_entry;

        }    
    }
    else if (rc == RC_ABORT)
    {
        for (vector<MvtoAccess*>::iterator ite = txn_state->accesses_.begin(); ite != txn_state->accesses_.end(); ite++)
        {
            MvtoAccess* access = *ite;
            
            delete access->req_entry;

        }   
    }
}



/************* MvtoTxnMeta *************/

MvtoTxnMeta::MvtoTxnMeta()
{
    timestamp_ = 0;
}


MvtoTxnMeta::~MvtoTxnMeta()
{

}


void MvtoTxnMeta::Reset()
{
    // timestamp_ = 0;
}


Timestamp MvtoTxnMeta::GetTimestamp()
{
    return timestamp_;
}




/*********** ReqEntry ************/

ReqEntry::ReqEntry(Tuple* tuple, AccessType access_type, Timestamp req_ts)
{
    this->access_type_ = access_type;
    this->req_ts_      = req_ts;
    
    this->req_ready_ = false;
    this->req_abort_ = false;

    this->visible_version_ = nullptr;
    this->new_version_     = tuple;

    this->next_ = nullptr;
}

ReqEntry::~ReqEntry()
{

}




/************ MvtoAccess ************/

MvtoAccess::~MvtoAccess()
{

}




/********** MvtoVersionMeta ***********/

MvtoVersionMeta::MvtoVersionMeta()
{
    blatch_ = false;

    latest_version_ = nullptr;
    version_num_    = 0;
    
    deleted_    = false;
    // delete_ts_  = 0;

    latest_wts_ = 0;
    oldest_wts_ = 0;
    max_rts_    = 0;

    exist_prewrite_ = false;
    pre_write_req_  = nullptr;

    requests_head_  = nullptr;
}

MvtoVersionMeta::MvtoVersionMeta(Tuple* tuple)
{
    blatch_ = false;

    latest_version_ = tuple;
    version_num_    = 0;
    
    deleted_    = false;
    // delete_ts_  = 0;

    latest_wts_ = 0;
    oldest_wts_ = 0;
    max_rts_    = 0;

    exist_prewrite_ = false;
    pre_write_req_  = nullptr;

    requests_head_  = nullptr;
}


MvtoVersionMeta::~MvtoVersionMeta()
{
    
}


void MvtoVersionMeta::GetLatch()
{
    while(!ATOM_CAS(blatch_, false, true)){}
}


void MvtoVersionMeta::ReleaseLatch()
{
    blatch_ = false;
}


RC MvtoVersionMeta::FindVisibleTuple(Timestamp ts, Tuple* &tuple)
{
    RC rc = RC_OK;
    MvtoTupleMeta* cc_tuple_meta = nullptr;

    tuple = latest_version_;
    
    while (tuple != nullptr)
    {
        cc_tuple_meta = tuple->cc_tuple_meta_;

        if (ts > cc_tuple_meta->w_ts_)
        {
            rc = RC_OK;
            break;
        }
        else
        {
            tuple = cc_tuple_meta->next_version_;
        }
    }
    
    if (tuple == nullptr)
        rc = RC_NULL;

    return rc;
}


RC MvtoVersionMeta::BufferRequest(ReqEntry* req_entry)
{
    RC rc = RC_OK;

    ReqEntry* cur_entry = requests_head_;
    ReqEntry* pre_entry = nullptr;

    while (cur_entry != nullptr)
    {
        if (cur_entry->req_ts_ < req_entry->req_ts_)
        {
            pre_entry = cur_entry;
            cur_entry = cur_entry->next_;
            continue;
        }
        else
        {
            break;
        }
    }
    
    if (cur_entry == requests_head_)
    {
        req_entry->next_ = requests_head_;
        requests_head_   = req_entry;
    }
    else
    {
        req_entry->next_ = cur_entry;
        pre_entry->next_ = req_entry;
    }

    return rc;
}


RC MvtoVersionMeta::RemoveRequest(ReqEntry* req_entry)
{
    RC rc = RC_OK;

    return rc;
}


RC MvtoVersionMeta::RequestAccess(ReqEntry* req_entry)
{
    RC rc = RC_OK;

    AccessType access_type = req_entry->access_type_;
    Timestamp  req_ts      = req_entry->req_ts_;

    GetLatch();

    if (access_type == READ_AT)
    {
        if (req_ts <= oldest_wts_)
            rc = RC_ABORT;
        else if (req_ts > latest_wts_)
        {
            if (exist_prewrite_ && pre_write_req_->req_ts_ < req_ts)
            {
                //need to buffer the read request
                //wait for prewrite finish
                BufferRequest(req_entry);
                rc = RC_WAIT;
            }
            else
            {
                //not exist prerwrite, or the ts of prewrite is larger than req_ts
                //return latest submitted tuple
                //if tuple has been deleted, current read txn would to abort
                if (deleted_)
                    rc = RC_ABORT;
                else
                {
                    req_entry->visible_version_ = latest_version_;
                    if (req_ts > max_rts_)
                        max_rts_ = req_ts;                 
                    rc = RC_OK;
                }
            }
        }
        else
        {
            //find consistent tuple version
            rc = FindVisibleTuple(req_ts, req_entry->visible_version_);
            if (rc == RC_NULL)
                printf("Error! FindVisibleTuple returns anexpected result!\n");
        }
    }
    else if (access_type == UPDATE_AT || access_type == DELETE_AT)
    {
        if (req_ts < latest_wts_ || req_ts < max_rts_ || (exist_prewrite_ && req_ts < pre_write_req_->req_ts_))
            rc = RC_ABORT;
        else if (exist_prewrite_)
        {
            BufferRequest(req_entry);
            rc = RC_WAIT;
        }
        else
        {
            exist_prewrite_   = true;
            pre_write_req_    = req_entry;
            req_entry->visible_version_ = latest_version_;
            rc = RC_OK;
        }
    }
    else if (access_type == INSERT_AT)
    {
        exist_prewrite_ = true;
        pre_write_req_  = req_entry;
        rc = RC_OK;
    }
    
    ReleaseLatch();

    return rc;
}


RC MvtoVersionMeta::FinishAccess(ReqEntry* req_entry, RC rc, Timestamp invisible_ts)
{
    AccessType access_type = req_entry->access_type_;

    GetLatch();

    if (rc == RC_COMMIT)
    {
        if (access_type == READ_AT)
        {
            ;
        }
        else if (access_type == UPDATE_AT 
                    || access_type == INSERT_AT)
        {
            latest_version_ = req_entry->new_version_;
            latest_wts_     = req_entry->req_ts_;
            max_rts_        = latest_wts_;
            version_num_++;

            if (access_type == INSERT_AT)
            {
                oldest_wts_ = req_entry->req_ts_;
            }

            exist_prewrite_ = false;
            pre_write_req_  = nullptr;
            
            while (requests_head_ != nullptr)
            {
                ReqEntry* cur_req = requests_head_;

                cur_req->visible_version_ = req_entry->new_version_;
                cur_req->req_ready_ = true;

                requests_head_ = cur_req->next_;

                if (cur_req->access_type_ == DELETE_AT
                    || cur_req->access_type_ == UPDATE_AT)
                {
                    exist_prewrite_ = true;
                    pre_write_req_  = cur_req;
                    break;
                }
            }

            if (access_type == UPDATE_AT)
            {
                // ClearInVisibleVersion(invisible_ts);
            }
            
        }
        else if (access_type == DELETE_AT)
        {
            deleted_    = true;
            latest_wts_ = req_entry->req_ts_;
            max_rts_    = latest_wts_;

            exist_prewrite_ = false;
            pre_write_req_  = nullptr;

            //abort all txns that are waiting!
            while (requests_head_ != nullptr)
            {
                requests_head_->req_abort_ = true;
                requests_head_ = requests_head_->next_;
            }
        }
    }
    else if (rc == RC_ABORT)
    {
        if (access_type == READ_AT)
        {
            ;
        }
        else if (access_type == UPDATE_AT || access_type == DELETE_AT)
        {
            exist_prewrite_ = false;
            pre_write_req_  = nullptr;
            
            while (requests_head_ != nullptr)
            {
                ReqEntry* cur_entry = requests_head_;

                cur_entry->visible_version_ = latest_version_;
                cur_entry->req_ready_ = true;

                requests_head_ = cur_entry->next_;

                if (cur_entry->access_type_ == DELETE_AT
                    || cur_entry->access_type_ == UPDATE_AT)
                {
                    exist_prewrite_ = true;
                    pre_write_req_  = cur_entry;
                    break;
                }
            }

            if (access_type == UPDATE_AT)
            {
                // ClearInVisibleVersion(invisible_ts);
            }

        }
        else if (access_type == INSERT_AT)
        {
            deleted_    = false;
            latest_wts_ = 0;

            exist_prewrite_ = false;
            pre_write_req_  = nullptr;

            while (requests_head_ != nullptr)
            {
                requests_head_->req_abort_ = true;
                requests_head_ = requests_head_->next_;
            }
        }
    }
        

    ReleaseLatch();

    return rc;
}


void MvtoVersionMeta::ClearInVisibleVersion(Timestamp invisible_ts)
{
    // printf("invisble_ts: %ld\n", invisible_ts);

    if (invisible_ts < oldest_wts_ || latest_version_ == nullptr)
    {
        return;
    }

    Tuple* cur_version = latest_version_;

    while (cur_version != nullptr && cur_version->cc_tuple_meta_->w_ts_ > invisible_ts)
    {
        cur_version = cur_version->cc_tuple_meta_->next_version_;
    }

    if (cur_version == nullptr)
    {
        return;
    }
    
    // printf("find oldest visible tuple!\n");

    // printf("find oldest visible tuple, ts is: %ld\n", cur_version->cc_tuple_meta_->w_ts_);
    
    Tuple* next_version;
    next_version  = cur_version->cc_tuple_meta_->next_version_;

    oldest_wts_ = cur_version->cc_tuple_meta_->w_ts_;
    cur_version->cc_tuple_meta_->next_version_ = nullptr;

    cur_version = next_version;

    while (cur_version != nullptr)
    {
        next_version = next_version->cc_tuple_meta_->next_version_;
        
        delete cur_version;
        version_num_--;

        cur_version = next_version;

        // printf("clear tuple!\n");
    }
    
    return;
}


RC MvtoVersionMeta::ReplayVersion(Tuple* tuple)
{
    GetLatch();
    
    Tuple* cur_version = latest_version_;
    
    while (cur_version != nullptr && cur_version->cc_tuple_meta_->w_ts_ != tuple->cc_tuple_meta_->w_ts_)
    {
        cur_version = cur_version->cc_tuple_meta_->next_version_;
    }
    
    ReleaseLatch();

    if (cur_version == nullptr)
    {
        return RC_NULL;
    }
    else
    {
        return RC_OK;
    }
}



/************ MvtoTupleMeta ************/

MvtoTupleMeta::MvtoTupleMeta()
{
    next_version_ = nullptr;
    w_ts_ = 0;
}


#endif
