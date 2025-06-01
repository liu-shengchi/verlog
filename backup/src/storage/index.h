#ifndef STORAGE_INDEX_H_
#define STORAGE_INDEX_H_

#include "config.h"


class AccessObj;


class Index
{
public:
    
    virtual RC  IndexInsert(IndexKey key, AccessObj* access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) = 0;

    virtual RC  IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) = 0;

    //for non-primary key index, return the set of tuples that equal to indexkey
    virtual RC  IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) = 0;

    // virtual RC  IndexRead(IndexKey key, AccessObj* &access_obj, Timestamp visible_ts) = 0;

    //针对part_ordered_index设计
    virtual RC  IndexMaxRead(IndexKey index_key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP){return RC_OK;};

    virtual RC  IndexRemove(IndexKey key, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) = 0;


protected:

    //indicate which table the index belongs to
    TableID     table_id_;

    //indek_key是否唯一
    bool        is_unique_index_;

};





#endif