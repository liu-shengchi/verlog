// #include "index_hash.h"

// #include "global.h"
// #include "table.h"
// #include "tuple.h"

// #include <malloc.h>



// /********** IndexHash **********/

// IndexHash::IndexHash(uint64_t  bucket_cnt, Table* table, bool is_pk_index)
// {
//     is_pk_index_ = is_pk_index;
//     table_ = table;
//     bucket_cnt_ = bucket_cnt;
    
//     buckets_ = new BucketHeader[bucket_cnt];
//     for(uint64_t i = 0; i < bucket_cnt_; i++)
//     {
//         buckets_[i].init();
//     }
// }

// IndexHash::IndexHash(uint64_t  bucket_cnt, TableID table_id, bool is_pk_index)
// {
//     bucket_cnt_  = bucket_cnt;
//     table_id_    = table_id;
//     is_pk_index_ = is_pk_index;
    
//     buckets_ = new BucketHeader[bucket_cnt];
//     for(uint64_t i = 0; i < bucket_cnt_; i++)
//     {
//         buckets_[i].init();
//     }
// }


// RC IndexHash::IndexInsert(IndexKey key, ItemPtr * item) {
//     RC rc = RC_OK;
//     uint64_t bkt_idx = hash(key);
//     // assert(bkt_idx < _bucket_cnt_per_part);
//     BucketHeader * cur_bkt = &buckets_[bkt_idx];
//     // 1. get the ex latch
//     // get_latch(cur_bkt);
    
//     cur_bkt->GetWriteLock();

//     // 2. update the latch list
//     cur_bkt->InsertTuple(key, item);

//     // 3. release the latch
//     // release_latch(cur_bkt);
//     cur_bkt->ReleaseWriteLock();

//     return rc;
// }



// RC IndexHash::IndexInsert(IndexKey key, Tuple* tuple)
// {
//     RC rc = RC_OK;
    
//     ItemPtr* item_ptr = new ItemPtr();

//     item_ptr->location = tuple;
//     item_ptr->type     = DT_TUPLE;
//     item_ptr->valid    = true;
//     item_ptr->next     = nullptr;
    
//     uint64_t bkt_idx = hash(key);
//     // assert(bkt_idx < _bucket_cnt_per_part);
//     BucketHeader * cur_bkt = &buckets_[bkt_idx];
//     // 1. get the ex latch
//     // get_latch(cur_bkt);
    
//     cur_bkt->GetWriteLock();

//     // 2. update the latch list
//     rc = cur_bkt->InsertTuple(key, item_ptr, is_pk_index_);

//     // 3. release the latch
//     // release_latch(cur_bkt);
//     cur_bkt->ReleaseWriteLock();

    

//     return rc; 
// }


// RC IndexHash::IndexRead(IndexKey key, ItemPtr * &item)
// {
//     uint64_t bkt_idx = hash(key);
//     // assert(bkt_idx < _bucket_cnt_per_part);
//     BucketHeader * cur_bkt = &buckets_[bkt_idx];
//     RC rc = RC_OK;

//     cur_bkt->GetReadLock();
//     cur_bkt->ReadTuple(key, item);    
//     cur_bkt->ReleaseReadLock();

//     return rc;
// }

// RC IndexHash::IndexRead(IndexKey key, Tuple* &tuple)
// {
//     RC rc = RC_OK;

//     uint64_t bkt_idx = hash(key);
//     BucketHeader * cur_bkt = &buckets_[bkt_idx];

//     ItemPtr* item_ptr = nullptr;

//     cur_bkt->GetReadLock();
//     cur_bkt->ReadTuple(key, item_ptr);
//     cur_bkt->ReleaseReadLock();

//     tuple = (Tuple*)item_ptr->location;

//     return rc;
// }


// RC IndexHash::IndexRead(IndexKey key, Tuple** &tuples, uint32_t &size)
// {
//     return RC_OK;
// }


// RC IndexHash::IndexRead(IndexKey key, vector<Tuple*>* tuples)
// {
//     return RC_OK;
// }


// //TODO
// RC IndexHash::IndexRemove(IndexKey key)
// {
//     RC rc = RC_OK;

//     uint64_t bkt_idx = hash(key);
//     BucketHeader * cur_bkt = &buckets_[bkt_idx];

//     cur_bkt->GetWriteLock();
//     cur_bkt->DeleteTuple(key); 
//     cur_bkt->ReleaseWriteLock();

//     return rc;
// }


// // void
// // IndexHash::get_latch(BucketHeader * bucket) {
// //     while (!ATOM_CAS(bucket->locked, false, true)) {}
// // }

// // void
// // IndexHash::release_latch(BucketHeader * bucket) {
// //     bool ok = ATOM_CAS(bucket->locked, true, false);
// //     // assert(ok);
// // }



// /************** BucketHeader Operations ******************/

// void BucketHeader::init()
// {
//     // node_cnt = 0;
//     first_node = NULL;
//     // locked = false;

//     write_lock_ = false;
//     read_cnt_   = 0;

// }


// void BucketHeader::ReadTuple(IndexKey key, ItemPtr * &item)
// {
//     BucketNode * cur_node = first_node;
//     while (cur_node != NULL) {
//         if (cur_node->key == key)
//             break;
//         cur_node = cur_node->next;
//         // printf(".....\n");
//     }
//     // M_ASSERT(cur_node->key == key, "Key does not exist!");
//     item = cur_node->items;
// }


// void BucketHeader::InsertTuple(IndexKey key, ItemPtr * item)
// {
//     BucketNode * cur_node = first_node;
//     BucketNode * prev_node = NULL;
//     while (cur_node != NULL) {
//         if (cur_node->key == key)
//             break;
//         prev_node = cur_node;
//         cur_node = cur_node->next;
//     }
//     if (cur_node == NULL) {
//         BucketNode * new_node = new BucketNode(key);
//         new_node->items = item;
//         if (prev_node != NULL) {
//             new_node->next = prev_node->next;
//             prev_node->next = new_node;
//         } else {
//             new_node->next = first_node;
//             first_node = new_node;
//         }
//     } else {

//         item->next = cur_node->items;
//         cur_node->items = item;
//     }
// }

// RC BucketHeader::InsertTuple(IndexKey key, ItemPtr * item, bool is_pk_index)
// {
//     BucketNode * cur_node = first_node;
//     BucketNode * prev_node = NULL;
//     while (cur_node != NULL) {
//         if (cur_node->key == key)
//             break;
//         prev_node = cur_node;
//         cur_node = cur_node->next;
//     }
//     if (cur_node == NULL) {
//         BucketNode * new_node = new BucketNode(key);
//         // new_node->key   = key;
//         new_node->items = item;
//         if (prev_node != NULL) {
//             new_node->next = prev_node->next;
//             prev_node->next = new_node;
//         } else {
//             new_node->next = first_node;
//             first_node = new_node;
//         }
//     } else {
//         if (is_pk_index)
//         {
//             // printf("return error! \n");
//             return RC_ERROR;
//         }
//         item->next = cur_node->items;
//         cur_node->items = item;
//     }
//     // printf("return ok! \n");
//     return RC_OK;
// }


// void BucketHeader::DeleteTuple(IndexKey key)
// {
//     BucketNode* cur_node  = first_node;
//     BucketNode* prev_node = NULL;
//     while (cur_node != NULL)
//     {
//         if (cur_node->key == key)
//             break;
//         prev_node = cur_node;
//         cur_node  = cur_node->next;
//     }
    
//     if (cur_node != NULL)
//     {
//         if (cur_node == first_node)
//             first_node = cur_node->next;
//         else
//             prev_node->next = cur_node->next;

//         delete cur_node->items;
//         delete cur_node;
//     }
// }



// void BucketHeader::GetReadLock()
// {
//     while (true)
//     {
//         while (!ATOM_CAS(write_lock_, false, false)) {}
        
//         ATOM_ADD(read_cnt_, 1);

//         if (!ATOM_CAS(write_lock_, false, false))
//         {
//             ATOM_SUB(read_cnt_, 1);
//             continue;
//         }
//         else
//         {
//             break;
//         }
//     }
// }

// void BucketHeader::ReleaseReadLock()
// {
//     ATOM_SUB(read_cnt_, 1);
// }

// void BucketHeader::GetWriteLock()
// {
//     while (!ATOM_CAS(write_lock_, false, true)) {}
    
//     while (!ATOM_CAS(read_cnt_, 0, 0)) {}

// }

// void BucketHeader::ReleaseWriteLock()
// {
//     ATOM_CAS(write_lock_, true, false);
// }
