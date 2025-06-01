#ifndef BENCHMARK_YCSB_SCHEMA_H_
#define BENCHMARK_YCSB_SCHEMA_H_


#include "schema.h"



#if   WORKLOAD_TYPE == YCSB_W


class YCSBSchema : public Schema
{
public:
    YCSBSchema();
    ~YCSBSchema();

    uint64_t GetTupleSize(TableID table_id);
    
    void     SetColumnValue(TableID table_type, ColumnID column_name, TupleData tuple_data, ColumnData column_data);
    uint64_t GetColumnValue(TableID table_type, ColumnID column_name, TupleData tuple_data, ColumnData &column_data);
    
    void     CreateIndex(IndexID index_id, ShardID shard_id);
    Index*   GetIndex(IndexID index_id, ShardID shard_id);

    IndexKey GetIndexKey(IndexID index_id, void * index_attr[]);
    IndexKey GetIndexKey(IndexID index_id, TupleData tuple_data);

    uint64_t FetchIncID(TableID table_id){return 0;}

};



#endif
#endif