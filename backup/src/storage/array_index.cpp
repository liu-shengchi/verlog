#include "array_index.h"

#include "queryfresh_am.h"

#if AM_STRATEGY_TYPE == QUERY_FRESH_AM

ArrayIndex::ArrayIndex(uint64_t array_length, TableID table_id, bool is_unique_index)
{
    table_id_        = table_id;
    is_unique_index_ = is_unique_index;
    array_length_    = array_length;
    array_           = new ReplayArrayAO[array_length_];
    for (uint64_t i = 0; i < array_length_; i++)
    {
        array_[i].spin_lock_.ReleaseSpinLock();
    }
    
}


RC ArrayIndex::IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key, Timestamp visible_ts)
{
    access_obj = &array_[key];
    return RC::RC_OK;
}

#endif