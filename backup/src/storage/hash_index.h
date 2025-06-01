#ifndef STORAGE_HASH_INDEX_H_
#define STORAGE_HASH_INDEX_H_

#include "index.h"

#include "db_rw_lock.h"


class AccessObj;


// each BucketNode contains items sharing the same key
class BucketNode {
public:
    BucketNode(IndexKey key) {	init(key); };
    void init(IndexKey key) {
        this->key       = key;
        diff_next_node_ = nullptr;
        same_next_node_ = nullptr;
    }
    
    IndexKey 	  key;
    
    // The next node that has different indexkey
    BucketNode*   diff_next_node_;

    // The next node that has same indexkey
    // if this index is primary index, it equals to nullptr
    BucketNode*   same_next_node_;

    AccessObj*    access_obj_;
};


// BucketHeader does concurrency control of Hash
// TODO efficient read & write concurrency mechanism
class BucketHeader {
public:
    void init();
    
    RC   ReadTuple(IndexKey key, AccessObj* &access_obj);
    
    //TODO
    //non-primary index read tuple
    // RC   ReadTuple(IndexKey key, Tuple** &tuple, uint64_t& tuple_count);

    RC   InsertTuple(IndexKey key, AccessObj* access_obj, bool is_unique_index);
    RC   RemoveTuple(IndexKey key);

    BucketNode* 	first_node;
    uint64_t 		node_cnt;

private:

    DBrwLock db_rw_lock;

};

/*
Hash Index
    Currently, HashIndex supports primary and secondary index, allows insertion 
and read useing single indexkey. Not allow range query.

*/
class HashIndex : public Index
{
public:

    HashIndex(uint64_t  bucket_cnt, TableID table_id, bool is_unique_index);

    RC IndexInsert(IndexKey key, AccessObj* access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    // the following call returns a single tuple
    RC IndexRead(IndexKey key, AccessObj* &access_obj, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    //TODO
    //the following call return a set of tuple that indexkey equals to key
    //use for secondary index
    RC IndexRead(IndexKey key, AccessObj** &access_objs, uint64_t& ao_cnt, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);

    //need to free memory that index data structure occupied
    //not free tuple. The lifecycle of a tuple is determined by the transaction component
    RC IndexRemove(IndexKey key, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);


private:

    // TODO implement more complex hash function
    uint64_t HashFunc(IndexKey key);
    
    BucketHeader* 	buckets_;
    uint64_t	 	bucket_cnt_;

};



#endif