#ifndef BENCHMARK_TPCC_SCHEMA_H_
#define BENCHMARK_TPCC_SCHEMA_H_

#include "config.h"
#include "schema.h"

#if   WORKLOAD_TYPE == TPCC_W


class TPCCSchema : public Schema
{
public:
    TPCCSchema();
    ~TPCCSchema();

    uint64_t GetTupleSize(TableID table_id);
    void     SetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData column_data);
    uint64_t GetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData &column_data);
    
    Index*   GetIndex(IndexID index_id, ShardID shard_id = 0);

    IndexKey GetIndexKey(IndexID index_id, void * index_attr[]);
    IndexKey GetIndexKey(IndexID index_id, TupleData tuple_data);
    
    PartitionKey GetPartKey(IndexID index_id, void* part_attr[]);
    PartitionKey GetPartKey(IndexID index_id, TupleData tuple_data);

    uint64_t FetchIncID(TableID table_id);

    void PrintTableCatalog();



#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    Index*   GetUnappliedIndex(TableID table_id, ShardID shard_id = 0);

#elif    AM_STRATEGY_TYPE == QUERY_FRESH_AM
    Index*   GetReplayArray(TableID table_id, ShardID shard_id = 0);
#endif

};


#endif
#endif