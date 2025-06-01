#include "ycsb_config.h"

#include "global.h"

#include "ycsb_schema.h"

#include "table.h"
#include "column.h"
#include "column.h"
#include "index.h"
#include "hash_index.h"
#include "array_index.h"

#include <stdio.h>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <cmath>

using namespace std;

#if   WORKLOAD_TYPE == YCSB_W


/******** TPCCSchema ********/

YCSBSchema::YCSBSchema()
{
    table_cnt_      = YCSB_TABLE_NUM;
    table_catalogs_ = new TableCatalog* [table_cnt_];

    for (uint32_t i = 0; i < YCSB_TABLE_NUM; i++)
    {
        table_catalogs_[i] = nullptr;
    }

    //initial table & column catalog

    string path = YCSB_SCHEMA_PATH;

    string line;
	ifstream fin(path.c_str());

    if (!fin.is_open())
    {
        printf("file open error! \n");
    }

    uint64_t table_num = 0;
    uint64_t index_num = 0;

    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {

            TableCatalog* table_catalog = new TableCatalog();
            table_catalogs_[table_num] = table_catalog;

            table_catalog->table_id_ = table_num;
            table_catalog->tuple_num_ = 0;
            table_catalog->inc_id_ = 1;

			getline(fin, line);
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}

            table_catalog->col_num_ = lines.size();
            table_catalog->col_catalog_ = new ColCatalog[table_catalog->col_num_];

            uint64_t col_num = 0;
            uint64_t col_offset = 0;
            uint64_t tuple_size = 0;

			for (uint32_t i = 0; i < lines.size(); i++) {
				string line = lines[i];
			    size_t pos = 0;
				string token;
				int elem_num = 0;
				int size = 0;
				string type;
				string name;
				while (line.length() != 0) {
					pos = line.find(",");
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					// default: assert(false);
					}
					elem_num ++;
				}
                
                DataType data_type;
                
                if (type == "int64_t")
                    data_type = DataType::INT64_DT;
                else if (type == "uint64_t")
                    data_type = DataType::UINT64_DT;
                else if (type == "double")
                    data_type = DataType::DOUBLE_DT;
                else if (type == "date")
                    data_type = DataType::DATE_DT;
                else if (type == "string")
                    data_type = DataType::STRING_DT;
                else
                    ;
                
                table_catalog->col_catalog_[col_num].table_id_ = table_num;
                table_catalog->col_catalog_[col_num].column_id_ = col_num;
                table_catalog->col_catalog_[col_num].data_type_ = data_type;
                table_catalog->col_catalog_[col_num].col_size_ = size;
                table_catalog->col_catalog_[col_num].col_offset_ = col_offset;
                
                col_offset += size;
                tuple_size += size;
                col_num++;
			}

            // /****** 创建主键列 ******/
            // table_catalog->col_catalog_[col_num].table_id_ = table_num;
            // table_catalog->col_catalog_[col_num].column_id_ = col_num;
            // table_catalog->col_catalog_[col_num].data_type_ = DataType::UINT64_DT;
            // table_catalog->col_catalog_[col_num].col_size_ = 8;
            // table_catalog->col_catalog_[col_num].col_offset_ = col_offset;
            // col_offset += 8;
            // tuple_size += 8;
            // col_num++;
            // /***********************/ 

            table_catalog->tuple_size_ = tuple_size;

            // printf("table_id: %ld; tuple_size: %ld \n", table_num, tuple_size);

            table_num++;
        }
        else if (line.compare(0, 6, "INDEX=") == 0){
            // getline(fin, line);
        }
    }
	fin.close();


#if   STORAGE_STRATEGY_TYPE == NO_SHARD_SS

    index_cnt_ = YCSB_INDEX_NUM;
    indexes_ = new Index* [index_cnt_];
    for (uint32_t i = 0; i < index_cnt_; i++)
    {
        indexes_[i] = nullptr;
    }

    uint64_t mt_forcast_num = pow(2, MT_ALL_BITS);
    indexes_[0] = new HashIndex(mt_forcast_num / HASH_FILL_FACTOR, YCSB_TABLE, true);

#elif STORAGE_STRATEGY_TYPE == SHARD_BY_YCSB_SHARD_SS
    shard_cnt_           = g_ycsb_shard_num;
    index_cnt_per_shard_ = YCSB_INDEX_NUM;
    index_cnt_           = shard_cnt_ * index_cnt_per_shard_;

    indexes_ = new Index* [index_cnt_];
    for (uint32_t i = 0; i < index_cnt_; i++)
    {
        indexes_[i] = nullptr;
        
        uint64_t mt_forcast_num = pow(2, MT_ALL_BITS);
        indexes_[i] = new HashIndex(mt_forcast_num / HASH_FILL_FACTOR, YCSB_TABLE, true);
    }

  #if AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    unapplied_indexes_ = new Index* [index_cnt_];
    for (uint32_t i = 0; i < index_cnt_; i++)
    {
        unapplied_indexes_[i] = nullptr;


        unapplied_indexes_[i] = new HashIndex(pow(2, MT_ALL_BITS), YCSB_TABLE, true);
        // uint64_t mt_forcast_num = pow(2, MT_ALL_BITS);
        // unapplied_indexes_[i] = new HashIndex(mt_forcast_num / HASH_FILL_FACTOR / 2, YCSB_TABLE, true);
    }

  #elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    replay_arrays_ = new Index* [index_cnt_ ];
    for (uint32_t i = 0; i < index_cnt_; i++)
    {
        replay_arrays_[i] = nullptr;
        
        uint64_t mt_forcast_num = pow(2, MT_ALL_BITS);

    #if   QUERY_FRESH_REPLAY_ARRAY
        replay_arrays_[i] = new ArrayIndex(mt_forcast_num / HASH_FILL_FACTOR, YCSB_TABLE, true);
    #else
        replay_arrays_[i] = new HashIndex(mt_forcast_num / HASH_FILL_FACTOR, YCSB_TABLE, true);
    #endif
    
    
    }

  #endif

#endif

}

YCSBSchema::~YCSBSchema()
{

}


uint64_t YCSBSchema::GetTupleSize(TableID table_id)
{
    return table_catalogs_[table_id]->tuple_size_;
}


void YCSBSchema::SetColumnValue(TableID table_type, ColumnID column_name, TupleData tuple_data, ColumnData column_data)
{
    uint64_t col_size;
    uint64_t col_offset;

    col_size   = table_catalogs_[table_type]->col_catalog_[column_name].col_size_;
    col_offset = table_catalogs_[table_type]->col_catalog_[column_name].col_offset_;

    memcpy(&tuple_data[col_offset], column_data, col_size);

}

uint64_t YCSBSchema::GetColumnValue(TableID table_type, ColumnID column_name, TupleData tuple_data, ColumnData &column_data)
{
    uint64_t col_size;
    uint64_t col_offset;
    col_size   = table_catalogs_[table_type]->col_catalog_[column_name].col_size_;
    col_offset = table_catalogs_[table_type]->col_catalog_[column_name].col_offset_;

    column_data = &tuple_data[col_offset];

    return col_size;
}



void YCSBSchema::CreateIndex(IndexID index_id, ShardID shard_id)
{
    switch (index_id)
    {
    case YCSB_INDEX:
        indexes_[shard_id * index_cnt_per_shard_ + index_id] = new HashIndex(g_ycsb_record_num, YCSB_TABLE, true);
        break;
    default:
        break;
    }
}


Index* YCSBSchema::GetIndex(IndexID index_id, ShardID shard_id)
{
#if STORAGE_STRATEGY_TYPE == SHARD_BY_YCSB_SHARD_SS
    index_id += shard_id * index_cnt_per_shard_;
#endif

    return indexes_[ index_id];
}

IndexKey YCSBSchema::GetIndexKey(IndexID index_id, void * index_attr[])
{
    IndexKey index_key = 0;
    index_key |= *(uint64_t *)index_attr[0];
    return index_key;
}

IndexKey YCSBSchema::GetIndexKey(IndexID index_id, TupleData tuple_data){
    IndexKey index_key = 0;
    ColumnData column_data;
    void* index_attr[1];

    g_schema->GetColumnValue(YCSB_TABLE,YCSB_KEY,tuple_data, column_data);
    uint64_t key_id = *(uint64_t *)column_data;
    index_attr[0] = &key_id;
    index_key     = g_schema->GetIndexKey(0,index_attr);
    return index_key;
}


#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

Index* YCSBSchema::GetUnappliedIndex(TableID table_id, ShardID shard_id)
{
    return unapplied_indexes_[table_id + shard_id * index_cnt_per_shard_];
}


#elif    AM_STRATEGY_TYPE == QUERY_FRESH_AM

Index* YCSBSchema::GetReplayArray(TableID table_id, ShardID shard_id)
{
    return replay_arrays_[table_id + shard_id * index_cnt_per_shard_];
}

#endif


#endif