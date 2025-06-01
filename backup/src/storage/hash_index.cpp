#include "hash_index.h"

#include "global.h"

#include "access_obj.h"

#include <malloc.h>


/***********************************************/
/****************** IndexHash ******************/
/***********************************************/
HashIndex::HashIndex(uint64_t  bucket_cnt, TableID table_id, bool is_unique_index)
{
    bucket_cnt_  = bucket_cnt;
    table_id_    = table_id;
    is_unique_index_ = is_unique_index;
    
    buckets_ = new BucketHeader[bucket_cnt_];
    for(uint64_t i = 0; i < bucket_cnt_; i++)
    {
        buckets_[i].init();
    }
}


RC HashIndex::IndexInsert(IndexKey key, AccessObj* access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;
    
    uint64_t bkt_idx = HashFunc(key);
    BucketHeader * cur_bkt = &buckets_[bkt_idx];

    rc = cur_bkt->InsertTuple(key, access_obj, is_unique_index_);

    return rc; 
}


RC HashIndex::IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    uint64_t bkt_idx = HashFunc(key);
    BucketHeader * cur_bkt = &buckets_[bkt_idx];

    rc = cur_bkt->ReadTuple(key, access_obj);

    return rc;
}


//TODO
RC HashIndex::IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    

    return rc;
}



RC HashIndex::IndexRemove(IndexKey key, PartitionKey part_key, Timestamp visible_ts)
{
    RC rc = RC_OK;

    uint64_t bkt_idx = HashFunc(key);
    BucketHeader * cur_bkt = &buckets_[bkt_idx];

    rc = cur_bkt->RemoveTuple(key); 
    
    return rc;
}

uint64_t HashIndex::HashFunc(IndexKey key)
{ 
    return (uint64_t )(key) % bucket_cnt_;
}


/************************************************/
/************ BucketHeader Operations ***********/
/************************************************/
void BucketHeader::init()
{
    first_node = nullptr;
    node_cnt   = 0;
}


RC BucketHeader::ReadTuple(IndexKey key, AccessObj* &access_obj)
{
    RC rc = RC_OK;

    db_rw_lock.GetReadLock();

    BucketNode * cur_node = first_node;
    while (cur_node != nullptr) {
        if (cur_node->key == key)
            break;
        cur_node = cur_node->diff_next_node_;
    }
    
    if (cur_node == nullptr)
    {
        access_obj = nullptr;
        rc = RC_NULL;
    }
    else
    {
        access_obj = cur_node->access_obj_;
        rc = RC_OK;
    }

    db_rw_lock.ReleaseReadLock();

    return rc;
}

RC BucketHeader::InsertTuple(IndexKey key, AccessObj* access_obj, bool is_unique_index)
{
    RC rc = RC_OK;

    db_rw_lock.GetWriteLock();

    BucketNode* cur_node = first_node;
    BucketNode* prev_node = nullptr;
    
    while (cur_node != nullptr) {
        
        if (cur_node->key == key)
            break;
        
        prev_node = cur_node;
        cur_node = cur_node->diff_next_node_;
    }

    if (cur_node == nullptr) {  //no tuple has same indexkay
        
        BucketNode* new_node = new BucketNode(key);
        new_node->access_obj_ = access_obj;

        if (prev_node == nullptr) {   //first_node == nullptr             
            new_node->diff_next_node_ = first_node;
            first_node = new_node;
        } else {
            new_node->diff_next_node_ = prev_node->diff_next_node_;
            prev_node->diff_next_node_ = new_node;
        }

    } else { //find tuple that has same indexkey
        
        //primary index, not allow!
        if (is_unique_index)
        {
            rc = RC_ERROR;
        }
        else
        {
            //secondary index
            BucketNode* new_node = new BucketNode(key);
            new_node->access_obj_ = access_obj;
            
            new_node->same_next_node_ = cur_node->same_next_node_;
            cur_node->same_next_node_ = new_node;
        }
    }

    db_rw_lock.ReleaseWriteLock();

    return rc;
}


RC BucketHeader::RemoveTuple(IndexKey key)
{
    RC rc = RC_OK;

    db_rw_lock.GetWriteLock();

    BucketNode* cur_node  = first_node;
    BucketNode* prev_node = nullptr;
    while (cur_node != nullptr)
    {
        if (cur_node->key == key)
            break;
        prev_node = cur_node;
        cur_node  = cur_node->diff_next_node_;
    }
    
    if (cur_node != nullptr)
    {
        if (cur_node == first_node)
            first_node = cur_node->diff_next_node_;
        else
            prev_node->diff_next_node_ = cur_node->diff_next_node_;

        //if this index is primary index, only remove cur_node
        //if this index is secondary index, need to remove all BucketNode that has same indexkey
        while (cur_node != nullptr)
        {
            prev_node = cur_node;
            cur_node = cur_node->same_next_node_;

            delete cur_node;
        }
    }
    else
    {
        rc = RC_NULL;
    }
    
    db_rw_lock.ReleaseWriteLock();
    
    return rc;
}
