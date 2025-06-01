#ifndef STORAGE_ARRAY_INDEX_H_
#define STORAGE_ARRAY_INDEX_H_

#include "config.h"

#include "index.h"

#if AM_STRATEGY_TYPE == QUERY_FRESH_AM

class ReplayArrayAO;


/* 
 * 当前设计中，ArrayIndex仅用于QueryFreshAM
 */
class ArrayIndex : public Index
{
public:

    ArrayIndex(uint64_t array_length, TableID table_id, bool is_unique_index);

    RC IndexInsert(IndexKey key, AccessObj* access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) {return RC::RC_OK;}

    // the following call returns a single tuple
    RC IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    RC IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) {return RC::RC_OK;}

    //need to free memory that index data structure occupied
    //not free tuple. The lifecycle of a tuple is determined by the transaction component
    RC IndexRemove(IndexKey key, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP) {return RC::RC_OK;}


private:

    uint64_t       array_length_;
    ReplayArrayAO* array_;

};


#endif
#endif