#ifndef STORAGE_TABLE_H_
#define STORAGE_TABLE_H_

#include "config.h"
#include "column.h"

// class Index;
// class ItemPtr;
// class Tuple;
// class TPCCSchema;
// class YCSBSchema;
// class StorageStrategy;


class TableCatalog
{
public:
    TableID  table_id_;
    uint64_t tuple_num_;
    uint64_t tuple_size_;

    uint64_t inc_id_;

    uint64_t    col_num_;
    ColCatalog* col_catalog_;


    //TODO: primary key

};






// class Table
// {

//     // friend void TPCCSchema::InitialStorage(StorageStrategy* storage_strategy);
//     // friend void YCSBSchema::InitialStorage(StorageStrategy* storage_strategy);

// private:
    
//     TableID table_id_;
    
//     uint64_t tuple_count_;
    
//     bool     has_inc_id_;
//     uint64_t inc_id_;

//     bool   has_primary_key_;
//     Index* primary_key_index_;
    
//     bool                  has_second_index_;
//     map<IndexID, Index*>* second_indexes_;


// public:
//     Table();
//     Table(TableID table_id, bool has_inc_id);

//     Table(TableID table_id, Index* primary_key_index, map<IndexID, Index*>* second_indexes, bool has_primary_key, bool has_second_index, bool has_inc_id);
//     ~Table();

//     // const char * get_table_name() { return NULL; };

//     uint64_t GetIncID();

//     TableID GetTableID();

//     RC ReadTupleByPK(IndexKey index_key, ItemPtr* &item_ptr);

//     RC InsertTuple(Tuple* tuple);

// };



#endif