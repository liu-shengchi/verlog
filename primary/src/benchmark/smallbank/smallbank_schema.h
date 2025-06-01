#ifndef BENCHMARK_SMALLBANK_SCHEMA_H_
#define BENCHMARK_SMALLBANK_SCHEMA_H_

#include "config.h"
#include "schema.h"

#if   WORKLOAD_TYPE == SMALLBANK_W

class SmallBankSchema : public Schema
{
public:
    SmallBankSchema();
    ~SmallBankSchema();

    uint64_t GetTupleSize(TableID table_id);
    void     SetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData column_data);
    uint64_t GetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData &column_data);
    
    
    void     CreateIndex(IndexID index_id, ShardID shard_id);

    Index*   GetIndex(IndexID index_id, ShardID shard_id);

    IndexKey GetIndexKey(IndexID index_id, void * index_attr[]);
    IndexKey GetIndexKey(IndexID index_id, TupleData tuple_data);
    
    uint64_t FetchIncID(TableID table_id);

    void PrintTableCatalog();

};


#endif
#endif