// #ifndef STORAGE_INDEX_HASH_H_
// #define STORAGE_INDEX_HASH_H_

// #include "index.h"
// #include <vector>

// using namespace std;

// class ItemPtr;
// class Table;


// // each BucketNode contains items sharing the same key
// class BucketNode {
// public:
//     BucketNode(IndexKey key) {	init(key); };
//     void init(IndexKey key) {
//         this->key = key;
//         next      = NULL;
//         items     = NULL;
//     }
//     IndexKey 		key;
//     // The node for the next key
//     BucketNode * 	next;
//     // NOTE. The items can be a list of items connected by the next pointer.
//     ItemPtr * 		items;
// };


// // BucketHeader does concurrency control of Hash
// // TODO efficient read & write concurrency mechanism
// class BucketHeader {
// public:
//     void init();
    

//     void ReadTuple(IndexKey key, ItemPtr * &item);
//     void InsertTuple(IndexKey key, ItemPtr * item);
//     RC   InsertTuple(IndexKey key, ItemPtr * item, bool is_pk_index);
//     void DeleteTuple(IndexKey key);


//     void GetReadLock();
//     void ReleaseReadLock();

//     void GetWriteLock();
//     void ReleaseWriteLock();

//     BucketNode * 	first_node;
//     uint64_t 		node_cnt;

//     // bool 			locked;

// private:

//     bool     volatile write_lock_;
//     uint64_t volatile read_cnt_;

// };

// /*TODO::
//  * 1. BucketHeader lock mechanism, shared_lock & exclusive_lock, improve concurrency
//  * 2. non-primary key operation, interface definition & function implementation
//  */
// class IndexHash  : public Index
// {
// public:

//     IndexHash(uint64_t  bucket_cnt, Table* table, bool is_pk_index);

//     IndexHash(uint64_t  bucket_cnt, TableID table_id, bool is_pk_index);


//     // bool 		index_exist(IndexKey key); // check if the key exist.
    
//     RC IndexInsert(IndexKey key, ItemPtr * item);
    
//     RC IndexInsert(IndexKey key, Tuple* tuple);


//     // the following call returns a single item
//     RC	 		IndexRead(IndexKey key, ItemPtr * &item);
//     RC          IndexRead(IndexKey key, Tuple* &tuple);
//     RC          IndexRead(IndexKey key, Tuple** &tuples, uint32_t &size);
    
//     RC          IndexRead(IndexKey key, vector<Tuple*>* tuples);


//     // TODO: remove a tuple from index
//     RC          IndexRemove(IndexKey key);


// private:
//     // void get_latch(BucketHeader * bucket);
//     // void release_latch(BucketHeader * bucket);

//     // TODO implement more complex hash function
//     uint64_t hash(IndexKey key) { return (uint64_t )(key) % bucket_cnt_; }

//     BucketHeader * 	buckets_;
//     uint64_t	 	bucket_cnt_;

// };



// #endif