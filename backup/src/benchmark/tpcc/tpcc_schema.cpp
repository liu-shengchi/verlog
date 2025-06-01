#include "tpcc_schema.h"

#include "global.h"

#include "tpcc_config.h"

#include "table.h"
#include "column.h"
#include "column.h"
#include "index.h"
#include "hash_index.h"
#include "part_ordered_index.h"
#include "array_index.h"

#include <stdio.h>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <cmath>

using namespace std;


#if   WORKLOAD_TYPE == TPCC_W


/******** TPCCSchema ********/

TPCCSchema::TPCCSchema()
{
    table_cnt_      = TPCC_TABLE_NUM;
    table_catalogs_ = new TableCatalog* [table_cnt_];

    for (uint32_t i = 0; i < TPCC_TABLE_NUM; i++)
    {
        table_catalogs_[i] = nullptr;
    }
    
    if (STORAGE_STRATEGY_TYPE == NO_SHARD_SS)
    {
        shard_cnt_           = 1;
        index_cnt_per_shard_ = TPCC_INDEX_NUM;
        index_cnt_           = TPCC_INDEX_NUM;

        indexes_ = new Index* [index_cnt_];
        for (uint32_t i = 0; i < index_cnt_; i++)
        {
            indexes_[i] = nullptr;
        }
    }
    else if (STORAGE_STRATEGY_TYPE == SHARD_BY_WAREHOUSE_SS)
    {
        shard_cnt_           = g_warehouse_num;
        index_cnt_per_shard_ = TPCC_INDEX_NUM;
        index_cnt_           = shard_cnt_ * index_cnt_per_shard_;

        indexes_ = new Index* [index_cnt_];
        for (uint32_t i = 0; i < index_cnt_; i++)
        {
            indexes_[i] = nullptr;
        }
    }


#if  AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    unapplied_indexes_ = new Index* [table_cnt_ * shard_cnt_];
#elif    AM_STRATEGY_TYPE == QUERY_FRESH_AM
  #if  QUERY_FRESH_REPLAY_ARRAY
    replay_arrays_ = new Index* [table_cnt_];
  #else
    replay_arrays_ = new Index* [table_cnt_ * shard_cnt_];
    // replay_arrays_ = new Index* [table_cnt_];
  #endif
#endif



    //initial table & column catalog

    string path = TPCC_SCHEMA_PATH;

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

            table_catalog->tuple_size_ = tuple_size;

            // printf("table_id: %ld; tuple_size: %ld \n", table_num, tuple_size);

            table_num++;
        }
        else if (line.compare(0, 6, "INDEX=") == 0){
            // getline(fin, line);
        }
    }
	fin.close();
    
    if (STORAGE_STRATEGY_TYPE == NO_SHARD_SS)
    {
        uint64_t w_forecast_num  = pow(2, W_ALL_BITS);
        uint64_t d_forecast_num  = pow(2, D_ALL_BITS);
        uint64_t c_forecast_num  = pow(2, C_ALL_BITS);
        uint64_t h_forecast_num  = pow(2, H_ALL_BITS);
        uint64_t no_forecast_num = pow(2, NO_ALL_BITS);
        uint64_t o_forecast_num  = pow(2, O_ALL_BITS);
        uint64_t ol_forecast_num = pow(2, OL_ALL_BITS);
        uint64_t s_forecast_num  = pow(2, S_ALL_BITS);
        uint64_t i_forecast_num  = pow(2, I_ALL_BITS);
        uint64_t o_cust_part_num = c_forecast_num;

        indexes_[0]  = new HashIndex(w_forecast_num / HASH_FILL_FACTOR, WAREHOUSE_T, true);
        indexes_[1]  = new HashIndex(d_forecast_num / HASH_FILL_FACTOR, DISTRICT_T, true);
        indexes_[2]  = new HashIndex(c_forecast_num / HASH_FILL_FACTOR, CUSTOMER_T, true);
        indexes_[3]  = new HashIndex(h_forecast_num / HASH_FILL_FACTOR, HISTORY_T, true);
        indexes_[4]  = new HashIndex(no_forecast_num / HASH_FILL_FACTOR, NEW_ORDER_T, true);
        indexes_[5]  = new HashIndex(o_forecast_num / HASH_FILL_FACTOR, ORDER_T, true);
        indexes_[6]  = new HashIndex(ol_forecast_num / HASH_FILL_FACTOR, ORDER_LINE_T, true);
        indexes_[7]  = new HashIndex(s_forecast_num / HASH_FILL_FACTOR, STOCK_T, true);
        indexes_[8]  = new HashIndex(i_forecast_num / HASH_FILL_FACTOR, ITEM_T, true);
        indexes_[9]  = new HashIndex(c_forecast_num / HASH_FILL_FACTOR, CUSTOMER_T, false);
        indexes_[10] = new PartOrderedindex(o_cust_part_num / HASH_FILL_FACTOR, ORDER_T, true);
    }
    else if (STORAGE_STRATEGY_TYPE == SHARD_BY_WAREHOUSE_SS)
    {
        uint64_t w_fore_num_per_ware  = 1;
        uint64_t d_fore_num_per_ware  = pow(2, D_W_ID_OFFSET);
        uint64_t c_fore_num_per_ware  = pow(2, C_W_ID_OFFSET);
        uint64_t h_fore_num_per_ware  = pow(2, H_ID_BITS);
        uint64_t no_fore_num_per_ware = pow(2, NO_W_ID_OFFSET);
        uint64_t o_fore_num_per_ware  = pow(2, O_W_ID_OFFSET);
        uint64_t ol_fore_num_per_ware = pow(2, OL_W_ID_OFFSET);
        uint64_t s_fore_num_per_ware  = pow(2, S_W_ID_OFFSET);
        uint64_t i_fore_num           = pow(2, I_ID_BITS);
        uint64_t o_cust_part_num_per_ware = c_fore_num_per_ware;

        HashIndex* item_index           = new HashIndex(i_fore_num / HASH_FILL_FACTOR, ITEM_T, true);

    #if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
        HashIndex* unapplied_item_index = new HashIndex(i_fore_num / HASH_FILL_FACTOR, ITEM_T, true);
    #endif

        for (size_t i = 0; i < shard_cnt_; i++)
        {
            indexes_[i * index_cnt_per_shard_ + 0] = new HashIndex(w_fore_num_per_ware / HASH_FILL_FACTOR, WAREHOUSE_T, true);
            indexes_[i * index_cnt_per_shard_ + 1] = new HashIndex(d_fore_num_per_ware / HASH_FILL_FACTOR, DISTRICT_T, true);
            indexes_[i * index_cnt_per_shard_ + 2] = new HashIndex(c_fore_num_per_ware / HASH_FILL_FACTOR, CUSTOMER_T, true);
            indexes_[i * index_cnt_per_shard_ + 3] = new HashIndex(h_fore_num_per_ware / HASH_FILL_FACTOR, HISTORY_T, true);
            indexes_[i * index_cnt_per_shard_ + 4] = new HashIndex(no_fore_num_per_ware / HASH_FILL_FACTOR, NEW_ORDER_T, true);
            indexes_[i * index_cnt_per_shard_ + 5] = new HashIndex(o_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_T, true);
            indexes_[i * index_cnt_per_shard_ + 6] = new HashIndex(ol_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_LINE_T, true);
            indexes_[i * index_cnt_per_shard_ + 7] = new HashIndex(s_fore_num_per_ware / HASH_FILL_FACTOR, STOCK_T, true);
            indexes_[i * index_cnt_per_shard_ + 8] = item_index;
            indexes_[i * index_cnt_per_shard_ + 9] = new HashIndex(c_fore_num_per_ware / HASH_FILL_FACTOR, CUSTOMER_T, false);
            indexes_[i * index_cnt_per_shard_ + 10] = new PartOrderedindex(o_cust_part_num_per_ware / HASH_FILL_FACTOR, ORDER_T, true);

        #if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
            unapplied_indexes_[i * table_cnt_ + 0] = new HashIndex(w_fore_num_per_ware / HASH_FILL_FACTOR, WAREHOUSE_T, true);
            unapplied_indexes_[i * table_cnt_ + 1] = new HashIndex(d_fore_num_per_ware / HASH_FILL_FACTOR, DISTRICT_T, true);
            unapplied_indexes_[i * table_cnt_ + 2] = new HashIndex(c_fore_num_per_ware / HASH_FILL_FACTOR, CUSTOMER_T, true);
            unapplied_indexes_[i * table_cnt_ + 3] = new HashIndex(h_fore_num_per_ware / HASH_FILL_FACTOR, HISTORY_T, true);
            unapplied_indexes_[i * table_cnt_ + 4] = new HashIndex(no_fore_num_per_ware / HASH_FILL_FACTOR, NEW_ORDER_T, true);
            unapplied_indexes_[i * table_cnt_ + 5] = new HashIndex(o_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_T, true);
            unapplied_indexes_[i * table_cnt_ + 6] = new HashIndex(ol_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_LINE_T, true);
            unapplied_indexes_[i * table_cnt_ + 7] = new HashIndex(s_fore_num_per_ware / HASH_FILL_FACTOR, STOCK_T, true);
            unapplied_indexes_[i * table_cnt_ + 8] = unapplied_item_index;
        #endif
        }
    }


#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM

  #if     QUERY_FRESH_REPLAY_ARRAY
    
    uint64_t w_forecast_num  = pow(2, W_ALL_BITS);
    uint64_t d_forecast_num  = pow(2, D_ALL_BITS);
    uint64_t c_forecast_num  = pow(2, C_ALL_BITS);
    uint64_t h_forecast_num  = pow(2, H_ALL_BITS);
    uint64_t no_forecast_num = pow(2, NO_ALL_BITS);
    uint64_t o_forecast_num  = pow(2, O_ALL_BITS);
    uint64_t ol_forecast_num = pow(2, OL_ALL_BITS);
    uint64_t s_forecast_num  = pow(2, S_ALL_BITS);
    uint64_t i_forecast_num  = pow(2, I_ALL_BITS);

    replay_arrays_[0] = new ArrayIndex(w_forecast_num / HASH_FILL_FACTOR, WAREHOUSE_T, true);
    replay_arrays_[1] = new ArrayIndex(d_forecast_num / HASH_FILL_FACTOR, DISTRICT_T, true);
    replay_arrays_[2] = new ArrayIndex(c_forecast_num / HASH_FILL_FACTOR, CUSTOMER_T, true);
    replay_arrays_[3] = new ArrayIndex(h_forecast_num / HASH_FILL_FACTOR, HISTORY_T, true);
    replay_arrays_[4] = new ArrayIndex(no_forecast_num / HASH_FILL_FACTOR, NEW_ORDER_T, true);
    replay_arrays_[5] = new ArrayIndex(o_forecast_num / HASH_FILL_FACTOR, ORDER_T, true);
    replay_arrays_[6] = new ArrayIndex(ol_forecast_num / HASH_FILL_FACTOR, ORDER_LINE_T, true);
    replay_arrays_[7] = new ArrayIndex(s_forecast_num / HASH_FILL_FACTOR, STOCK_T, true);
    replay_arrays_[8] = new ArrayIndex(i_forecast_num / HASH_FILL_FACTOR, ITEM_T, true);

  #else

    uint64_t w_fore_num_per_ware  = 1;
    uint64_t d_fore_num_per_ware  = pow(2, D_W_ID_OFFSET);
    uint64_t c_fore_num_per_ware  = pow(2, C_W_ID_OFFSET);
    uint64_t h_fore_num_per_ware  = pow(2, H_ID_BITS);
    uint64_t no_fore_num_per_ware = pow(2, NO_W_ID_OFFSET);
    uint64_t o_fore_num_per_ware  = pow(2, O_W_ID_OFFSET);
    uint64_t ol_fore_num_per_ware = pow(2, OL_W_ID_OFFSET);
    uint64_t s_fore_num_per_ware  = pow(2, S_W_ID_OFFSET);
    uint64_t i_fore_num           = pow(2, I_ID_BITS);
    uint64_t o_cust_part_num_per_ware = c_fore_num_per_ware;

    HashIndex* item_index = new HashIndex(i_fore_num / HASH_FILL_FACTOR, ITEM_T, true);

    for (size_t i = 0; i < shard_cnt_; i++)
    {
        replay_arrays_[i * table_cnt_ + 0] = new HashIndex(w_fore_num_per_ware / HASH_FILL_FACTOR, WAREHOUSE_T, true);
        replay_arrays_[i * table_cnt_ + 1] = new HashIndex(d_fore_num_per_ware / HASH_FILL_FACTOR, DISTRICT_T, true);
        replay_arrays_[i * table_cnt_ + 2] = new HashIndex(c_fore_num_per_ware / HASH_FILL_FACTOR, CUSTOMER_T, true);
        replay_arrays_[i * table_cnt_ + 3] = new HashIndex(h_fore_num_per_ware / HASH_FILL_FACTOR, HISTORY_T, true);
        replay_arrays_[i * table_cnt_ + 4] = new HashIndex(no_fore_num_per_ware / HASH_FILL_FACTOR, NEW_ORDER_T, true);
        replay_arrays_[i * table_cnt_ + 5] = new HashIndex(o_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_T, true);
        replay_arrays_[i * table_cnt_ + 6] = new HashIndex(ol_fore_num_per_ware / HASH_FILL_FACTOR, ORDER_LINE_T, true);
        replay_arrays_[i * table_cnt_ + 7] = new HashIndex(s_fore_num_per_ware / HASH_FILL_FACTOR, STOCK_T, true);
        replay_arrays_[i * table_cnt_ + 8] = item_index;
    }
    

    // uint64_t w_forecast_num  = pow(2, W_ALL_BITS);
    // uint64_t d_forecast_num  = pow(2, D_ALL_BITS);
    // uint64_t c_forecast_num  = pow(2, C_ALL_BITS);
    // uint64_t h_forecast_num  = pow(2, H_ALL_BITS);
    // uint64_t no_forecast_num = pow(2, NO_ALL_BITS);
    // uint64_t o_forecast_num  = pow(2, O_ALL_BITS);
    // uint64_t ol_forecast_num = pow(2, OL_ALL_BITS);
    // uint64_t s_forecast_num  = pow(2, S_ALL_BITS);
    // uint64_t i_forecast_num  = pow(2, I_ALL_BITS);

    // replay_arrays_[0] = new HashIndex(w_forecast_num / HASH_FILL_FACTOR, WAREHOUSE_T, true);
    // replay_arrays_[1] = new HashIndex(d_forecast_num / HASH_FILL_FACTOR, DISTRICT_T, true);
    // replay_arrays_[2] = new HashIndex(c_forecast_num / HASH_FILL_FACTOR, CUSTOMER_T, true);
    // replay_arrays_[3] = new HashIndex(h_forecast_num / HASH_FILL_FACTOR, HISTORY_T, true);
    // replay_arrays_[4] = new HashIndex(no_forecast_num / HASH_FILL_FACTOR, NEW_ORDER_T, true);
    // replay_arrays_[5] = new HashIndex(o_forecast_num / HASH_FILL_FACTOR, ORDER_T, true);
    // replay_arrays_[6] = new HashIndex(ol_forecast_num / HASH_FILL_FACTOR, ORDER_LINE_T, true);
    // replay_arrays_[7] = new HashIndex(s_forecast_num / HASH_FILL_FACTOR, STOCK_T, true);
    // replay_arrays_[8] = new HashIndex(i_forecast_num / HASH_FILL_FACTOR, ITEM_T, true);

  #endif

#endif

}

TPCCSchema::~TPCCSchema()
{

}



uint64_t TPCCSchema::GetTupleSize(TableID table_id)
{
    return table_catalogs_[table_id]->tuple_size_;
}


void TPCCSchema::SetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData column_data)
{
    uint64_t col_size;
    uint64_t col_offset;

    col_size   = table_catalogs_[table_id]->col_catalog_[column_id].col_size_;
    col_offset = table_catalogs_[table_id]->col_catalog_[column_id].col_offset_;

    memcpy(&tuple_data[col_offset], column_data, col_size);

}

uint64_t TPCCSchema::GetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData &column_data)
{
    uint64_t col_size;
    uint64_t col_offset;
    col_size   = table_catalogs_[table_id]->col_catalog_[column_id].col_size_;
    col_offset = table_catalogs_[table_id]->col_catalog_[column_id].col_offset_;

    column_data = &tuple_data[col_offset];

    return col_size;
}


Index* TPCCSchema::GetIndex(IndexID index_id, ShardID shard_id)
{
#if STORAGE_STRATEGY_TYPE == SHARD_BY_WAREHOUSE_SS
    index_id += shard_id * index_cnt_per_shard_;
#endif

    return indexes_[index_id];
}


IndexKey TPCCSchema::GetIndexKey(IndexID index_id, void * index_attr[])
{
    IndexKey index_key = 0;

    switch (index_id)
    {
    case W_PK_INDEX:
        /** w_id: 0 - 63 **/
        index_key |= *(uint64_t *)index_attr[0];
        break;
    case D_PK_INDEX:
        /** w_id: 8 - 63 
         *  d_id: 0 - 7**/
        index_key |= (*(uint64_t *)index_attr[0]) << D_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]);
        break;
    case C_PK_INDEX:
        /** w_id: 32 - 63
         *  d_id: 16 - 31
         *  c_id: 0 - 15**/
        index_key |= (*(uint64_t *)index_attr[0]) << C_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]) << C_D_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[2]);
        break;
    case H_PK_INDEX:
        /** H_ID: 0 - 63 **/
        index_key |= (*(uint64_t *)index_attr[0]);
        break;
    case NO_PK_INDEX:
        /** w_id: 48 - 63
         *  d_id: 32 - 47
         *  o_id: 0 - 31**/
        index_key |= (*(uint64_t *)index_attr[0]) << NO_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]) << NO_D_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[2]);
        break;
    case O_PK_INDEX:
        /** w_id: 48 - 63
         *  d_id: 32 - 47
         *  o_id: 0 - 31**/
        index_key |= (*(uint64_t *)index_attr[0]) << O_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]) << O_D_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[2]);
        break;
    case OL_PK_INDEX:
        /** w_id: 48 - 63
         *  d_id: 40 - 47
         *  0_id: 8 - 39
         *  ol_num: 0 - 7**/
        index_key |= (*(uint64_t *)index_attr[0]) << OL_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]) << OL_D_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[2]) << OL_O_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[3]);
        break;
    case S_PK_INDEX:
        /** w_id: 48 - 63
         *  i_id: 0 - 47**/
        index_key |= (*(uint64_t *)index_attr[0]) << S_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]);
        break;
    case I_PK_INDEX:
        /** i_id: 0 - 63 **/
        index_key |= (*(uint64_t *)index_attr[0]);
        break;
    case C_LAST_INDEX:
        {
            /**  **/
            uint64_t c_w_id = *(uint64_t *)index_attr[0];
            uint64_t c_d_id = *(uint64_t *)index_attr[1];
            char* c_last = (char*)index_attr[2];
            char offset = 'A';
            for (uint32_t i = 0; i < strlen(c_last); i++) 
                index_key = (index_key << 2) + (c_last[i] - offset);
                index_key = index_key << 3;
            index_key += c_w_id * g_dist_per_ware + c_d_id;
            break;
        }
    case O_CUST_INDEX:
        /** w_id: O_C_W_ID_OFFSET - 63
         *  d_id: O_C_D_ID_OFFSET - O_C_W_ID_OFFSET-1
         *  c_id: O_C_C_ID_OFFSET - O_C_C_D_ID_OFFSET-1
         *  o_id: 0               - O_C_O_ID_BITS-1**/
        index_key |= (*(uint64_t *)index_attr[0]) << O_C_W_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[1]) << O_C_D_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[2]) << O_C_C_ID_OFFSET;
        index_key |= (*(uint64_t *)index_attr[3]);

    default:
        break;
    }
    
    return index_key;
}


IndexKey TPCCSchema::GetIndexKey(IndexID index_id, TupleData tuple_data)
{
    IndexKey index_key = 0;

    ColumnData column_data;

    void* index_attr[5];

    switch (index_id)
    {
    case W_PK_INDEX:
        {
            /** W_ID **/
            g_schema->GetColumnValue(WAREHOUSE_T, W_ID, tuple_data, column_data);
            uint64_t w_id = *(uint64_t *)column_data;
            
            index_attr[0] = &w_id;
            index_key     = GetIndexKey(W_PK_INDEX, index_attr);
            break;
        }
    case D_PK_INDEX:
        {
            /** D_W_ID, D_ID **/
            g_schema->GetColumnValue(DISTRICT_T, D_W_ID, tuple_data, column_data);
            uint64_t d_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(DISTRICT_T, D_ID, tuple_data, column_data);
            uint64_t d_id   = *(uint64_t *)column_data;
            
            index_attr[0] = &d_w_id;
            index_attr[1] = &d_id;
            index_key     = GetIndexKey(D_PK_INDEX, index_attr);

            break;
        }
    case C_PK_INDEX:
        {
            /** C_W_ID, C_D_ID, C_ID **/
            g_schema->GetColumnValue(CUSTOMER_T, C_W_ID, tuple_data, column_data);
            uint64_t c_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(CUSTOMER_T, C_D_ID, tuple_data, column_data);
            uint64_t c_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(CUSTOMER_T, C_ID, tuple_data, column_data);
            uint64_t c_id   = *(uint64_t *)column_data;

            index_attr[0] = &c_w_id;
            index_attr[1] = &c_d_id;
            index_attr[2] = &c_id;
            index_key     = GetIndexKey(C_PK_INDEX, index_attr);

            break;
        }
    case H_PK_INDEX:
        {
            /** H_ID **/
            g_schema->GetColumnValue(HISTORY_T, H_ID, tuple_data, column_data);
            uint64_t h_id = *(uint64_t *)column_data;

            index_attr[0] = &h_id;
            index_key     = GetIndexKey(H_PK_INDEX, index_attr);

            break;
        }
    case NO_PK_INDEX:
        {
            /** NO_W_ID, NO_D_ID, NO_O_ID **/
            g_schema->GetColumnValue(NEW_ORDER_T, NO_W_ID, tuple_data, column_data);
            uint64_t no_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(NEW_ORDER_T, NO_D_ID, tuple_data, column_data);
            uint64_t no_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(NEW_ORDER_T, NO_O_ID, tuple_data, column_data);
            uint64_t no_o_id = *(uint64_t *)column_data;

            index_attr[0] = &no_w_id;
            index_attr[1] = &no_d_id;
            index_attr[2] = &no_o_id;
            index_key     = GetIndexKey(NO_PK_INDEX, index_attr);

            break;
        }
    case O_PK_INDEX:
        {
            /** O_W_ID, O_D_ID, O_ID **/
            g_schema->GetColumnValue(ORDER_T, O_W_ID, tuple_data, column_data);
            uint64_t o_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_D_ID, tuple_data, column_data);
            uint64_t o_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_ID, tuple_data, column_data);
            uint64_t o_id   = *(uint64_t *)column_data;

            index_attr[0] = &o_w_id;
            index_attr[1] = &o_d_id;
            index_attr[2] = &o_id;
            index_key     = GetIndexKey(O_PK_INDEX, index_attr);

            break;
        }

    case OL_PK_INDEX:
        {
            /** OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER **/
            g_schema->GetColumnValue(ORDER_LINE_T, OL_W_ID, tuple_data, column_data);
            uint64_t ol_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_LINE_T, OL_D_ID, tuple_data, column_data);
            uint64_t ol_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_LINE_T, OL_O_ID, tuple_data, column_data);
            uint64_t ol_o_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_LINE_T, OL_NUMBER, tuple_data, column_data);
            uint64_t ol_num  = *(uint64_t *)column_data;

            index_attr[0] = &ol_w_id;
            index_attr[1] = &ol_d_id;
            index_attr[2] = &ol_o_id;
            index_attr[3] = &ol_num;
            index_key     = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);

            break;
        }
    case S_PK_INDEX:
        {
            /** S_W_ID, S_I_ID **/
            g_schema->GetColumnValue(STOCK_T, S_W_ID, tuple_data, column_data);
            uint64_t s_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(STOCK_T, S_I_ID, tuple_data, column_data);
            uint64_t s_i_id = *(uint64_t *)column_data;
            
            index_attr[0] = &s_w_id;
            index_attr[1] = &s_i_id;
            index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
            break;
        }
    case I_PK_INDEX:
        {
            /** I_ID **/
            g_schema->GetColumnValue(ITEM_T, I_ID, tuple_data, column_data);
            uint64_t i_id = *(uint64_t *)column_data;

            index_attr[0] = &i_id;
            index_key     = g_schema->GetIndexKey(I_PK_INDEX, index_attr);

            break;
        }
    case C_LAST_INDEX:
        {        
            /** C_W_ID, C_D_ID, C_LAST **/
            g_schema->GetColumnValue(CUSTOMER_T, C_W_ID, tuple_data, column_data);
            uint64_t c_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(CUSTOMER_T, C_D_ID, tuple_data, column_data);
            uint64_t c_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(CUSTOMER_T, C_LAST, tuple_data, column_data);
            char* c_last = column_data;

            index_attr[0] = &c_w_id;
            index_attr[1] = &c_d_id;
            index_attr[2] = c_last;
            index_key     = g_schema->GetIndexKey(C_LAST_INDEX, index_attr);

            break;
        }
    case O_CUST_INDEX:
        {
            /** O_W_ID, O_D_ID, O_ID **/
            g_schema->GetColumnValue(ORDER_T, O_W_ID, tuple_data, column_data);
            uint64_t o_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_D_ID, tuple_data, column_data);
            uint64_t o_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_C_ID, tuple_data, column_data);
            uint64_t o_c_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_ID, tuple_data, column_data);
            uint64_t o_id   = *(uint64_t *)column_data;

            index_attr[0] = &o_w_id;
            index_attr[1] = &o_d_id;
            index_attr[2] = &o_c_id;
            index_attr[3] = &o_id;
            index_key     = GetIndexKey(O_CUST_INDEX, index_attr);

            break;
        }

    default:
        break;
    }

    return index_key;
}


PartitionKey TPCCSchema::GetPartKey(IndexID index_id, void* part_attr[])
{
    PartitionKey part_key = 0;

    switch (index_id)
    {
    case O_CUST_INDEX:
        part_key |= (*(uint64_t*)part_attr[0]) << (O_C_W_ID_OFFSET - O_C_O_ID_BITS);
        part_key |= (*(uint64_t*)part_attr[1]) << (O_C_D_ID_OFFSET - O_C_O_ID_BITS);
        part_key |= (*(uint64_t*)part_attr[2]) << (O_C_C_ID_OFFSET - O_C_O_ID_BITS);
        break;
        
    default:
        break;
    }

    return part_key;
}


PartitionKey TPCCSchema::GetPartKey(IndexID index_id, TupleData tuple_data)
{
    IndexKey part_key = 0;

    ColumnData column_data;
    
    void* part_attr[5];

    switch (index_id)
    {
    case O_CUST_INDEX:
        {
            /** O_W_ID, O_D_ID, O_C_ID **/
            g_schema->GetColumnValue(ORDER_T, O_W_ID, tuple_data, column_data);
            uint64_t o_w_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_D_ID, tuple_data, column_data);
            uint64_t o_d_id = *(uint64_t *)column_data;
            g_schema->GetColumnValue(ORDER_T, O_C_ID, tuple_data, column_data);
            uint64_t o_c_id = *(uint64_t *)column_data;

            part_attr[0] = &o_w_id;
            part_attr[1] = &o_d_id;
            part_attr[2] = &o_c_id;
            part_key     = GetPartKey(index_id, part_attr);
            
            break;
        }
    default:
        break;
    }
    
    return part_key;
}


uint64_t TPCCSchema::FetchIncID(TableID table_id)
{
    TableCatalog* table_catalog = table_catalogs_[table_id];
    return ATOM_FETCH_ADD(table_catalog->inc_id_, 1);
}

void TPCCSchema::PrintTableCatalog()
{
    for (uint64_t i = 0; i < TPCC_TABLE_NUM; i++)
    {
        if (table_catalogs_[i] == nullptr)
        {
            printf("null\n");
        }

        printf("table id: %d, tuple num: %ld, tuple size: %ld, col num: %ld \n", table_catalogs_[i]->table_id_, table_catalogs_[i]->tuple_num_, table_catalogs_[i]->tuple_size_, table_catalogs_[i]->col_num_);
    }
}



#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

Index* TPCCSchema::GetUnappliedIndex(TableID table_id, ShardID shard_id)
{
    return unapplied_indexes_[shard_id * table_cnt_ + table_id];
}


#elif  AM_STRATEGY_TYPE == QUERY_FRESH_AM

Index* TPCCSchema::GetReplayArray(TableID table_id, ShardID shard_id)
{
  #if    QUERY_FRESH_REPLAY_ARRAY
    return replay_arrays_[table_id];
  #else

    return replay_arrays_[table_id + table_cnt_ * shard_id];
    // return replay_arrays_[table_id];

  #endif

}

#endif



#endif