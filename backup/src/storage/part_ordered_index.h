#ifndef STORAGE_PART_ORDERED_INDEX_H_
#define STORAGE_PART_ORDERED_INDEX_H_

#include "config.h"

#include "index.h"

#include "db_rw_lock.h"


class AccessObj;


class OrderedNode
{
public:
    OrderedNode(IndexKey index_key, Timestamp visible_ts, AccessObj* access_obj);
    ~OrderedNode();

    int  compare(IndexKey index_key);

    bool IsVisible(Timestamp visible_ts);

    IndexKey  index_key_;
    Timestamp visible_ts_;

    AccessObj* access_obj_;

    OrderedNode*   diff_next_node_;

    OrderedNode*   same_next_node_;


};




class OrderedIndex
{
private:
    
    DBrwLock   db_rw_lock_;

    OrderedNode* first_node_;


public:
    OrderedIndex();
    ~OrderedIndex();

    RC   ReadAccessObj(IndexKey index_key, AccessObj* &access_obj, Timestamp visible_ts);
    RC   InsertAccessObj(IndexKey index_key, AccessObj* access_obj, Timestamp visible_ts, bool is_unique_index);
    RC   RemoveAccessObj(IndexKey index_key);

};



class PartOrderedindex : public Index
{
private:
    
    uint64_t PartHashFunc(PartitionKey part_key);

    OrderedIndex*   ordered_indexes_;
    uint64_t        partition_cnt_;


public:
    PartOrderedindex(uint64_t partition_cnt, TableID table_id, bool is_unique_index);
    ~PartOrderedindex();

    RC  IndexInsert(IndexKey key, AccessObj* access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    RC  IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    //for non-primary key index, return the set of tuples that equal to indexkey
    RC  IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    /* 
     * 找到可见时间戳 <= visible_ts的，索引键小于等于index_key的最大AccessObj
     */
    RC  IndexMaxRead(IndexKey index_key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    RC  IndexRemove(IndexKey key, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);
    
};



#endif