#ifndef BENCHMARK_SCHEMA_H_
#define BENCHMARK_SCHEMA_H_


#include "config.h"


class TableCatalog;
class Index;


class Schema
{
public:

    uint64_t       table_cnt_;
    TableCatalog** table_catalogs_;

    uint64_t       shard_cnt_;
    uint64_t       index_cnt_per_shard_;
    uint64_t       index_cnt_;
    Index**        indexes_;


    virtual uint64_t GetTupleSize(TableID table_id) = 0;
    virtual void     SetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData column_data) = 0;
    virtual uint64_t GetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData &column_data) = 0;
    
    virtual void     CreateIndex(IndexID index_id, ShardID shard_id) = 0;

    virtual Index*   GetIndex(IndexID index_id, ShardID shard_id) = 0;
    virtual IndexKey GetIndexKey(IndexID index_id, void * index_attr[]) = 0;
    virtual IndexKey GetIndexKey(IndexID index_id, TupleData tuple_data) = 0;

    virtual uint64_t FetchIncID(TableID table_id) = 0;
    
};




#endif