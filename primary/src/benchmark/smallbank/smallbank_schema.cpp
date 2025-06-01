#include "smallbank_schema.h"

#include "global.h"

#include "smallbank_config.h"

#include "table.h"
#include "column.h"
#include "column.h"
#include "index.h"
#include "hash_index.h"

#include <stdio.h>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <cmath>

using namespace std;


#if   WORKLOAD_TYPE == SMALLBANK_W



/******** SmallBankSchema ********/

SmallBankSchema::SmallBankSchema()
{
    table_cnt_      = g_smallbank_table_num;
    table_catalogs_ = new TableCatalog* [table_cnt_];

    for (uint32_t i = 0; i < table_cnt_; i++)
    {
        table_catalogs_[i] = nullptr;
    }


    //initial table & column catalog

    string path = SMALLBANK_SCHEMA_PATH;

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


#if STORAGE_STRATEGY_TYPE == SHARD_BY_SMALLBANK_ACCOUNT_SS

    shard_cnt_           = g_smallbank_shard_num;
    index_cnt_per_shard_ = g_smallbank_index_num;
    index_cnt_ = shard_cnt_ * index_cnt_per_shard_;

    indexes_ = new Index* [index_cnt_];
    for (uint32_t i = 0; i < index_cnt_; i++)
    {
        indexes_[i] = nullptr;
    }

    //在Load阶段创建索引

#else
    printf("error! undefined storage strategy type in smallbank!\n");
    exit(0);
#endif

}

SmallBankSchema::~SmallBankSchema()
{

}



uint64_t SmallBankSchema::GetTupleSize(TableID table_id)
{
    return table_catalogs_[table_id]->tuple_size_;
}


void SmallBankSchema::SetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData column_data)
{
    uint64_t col_size;
    uint64_t col_offset;

    col_size   = table_catalogs_[table_id]->col_catalog_[column_id].col_size_;
    col_offset = table_catalogs_[table_id]->col_catalog_[column_id].col_offset_;

    memcpy(&tuple_data[col_offset], column_data, col_size);

}

uint64_t SmallBankSchema::GetColumnValue(TableID table_id, ColumnID column_id, TupleData tuple_data, ColumnData &column_data)
{
    uint64_t col_size;
    uint64_t col_offset;
    col_size   = table_catalogs_[table_id]->col_catalog_[column_id].col_size_;
    col_offset = table_catalogs_[table_id]->col_catalog_[column_id].col_offset_;

    column_data = &tuple_data[col_offset];

    return col_size;
}


void SmallBankSchema::CreateIndex(IndexID index_id, ShardID shard_id)
{

    switch (index_id)
    {
    case A_PK_INDEX:
        {
            uint64_t a_fore_num_per_shard = pow(2, A_NAME_BITS);
            indexes_[shard_id * index_cnt_per_shard_ + index_id] = new HashIndex(a_fore_num_per_shard / HASH_FILL_FACTOR, SMALLBANKTableType::ACCOUNT_T, true);
        }
        break;

    case S_PK_INDEX:
        {
            uint64_t s_fore_num_per_shard = pow(2, S_CUSTOMER_ID_BITS);
            indexes_[shard_id * index_cnt_per_shard_ + index_id] = new HashIndex(s_fore_num_per_shard / HASH_FILL_FACTOR, SMALLBANKTableType::SAVING_T, true);
        }
        break;
        
	case C_PK_INDEX:
        {
            uint64_t c_fore_num_per_shard = pow(2, C_CUSTOMER_ID_BITS);
            indexes_[shard_id * index_cnt_per_shard_ + index_id] = new HashIndex(c_fore_num_per_shard / HASH_FILL_FACTOR, SMALLBANKTableType::CHECKING_T, true);
        }
        break;

    default:
        break;
    }
}



Index* SmallBankSchema::GetIndex(IndexID index_id, ShardID shard_id = 0)
{
#if STORAGE_STRATEGY_TYPE == SHARD_BY_SMALLBANK_ACCOUNT_SS
    index_id += shard_id * index_cnt_per_shard_;
#endif

    return indexes_[index_id];
}


IndexKey SmallBankSchema::GetIndexKey(IndexID index_id, void * index_attr[])
{
    IndexKey index_key = 0;

    switch (index_id)
    {
    case A_PK_INDEX:
        /** a_name: 0 - 63 **/
        index_key |= *(uint64_t *)index_attr[0];
        break;
    case S_PK_INDEX:
        /** s_customer_id: 0 - 63 **/
        index_key |= (*(uint64_t *)index_attr[0]);
        break;
    case C_PK_INDEX:
        /** c_customer_id: 0 - 63 **/
        index_key |= (*(uint64_t *)index_attr[0]);
        break;
    default:
        printf("SmallBankSchema::GetIndexKey: undefined index type in smallbank!\n");
        exit(0);
        break;
    }
    
    return index_key;
}


IndexKey SmallBankSchema::GetIndexKey(IndexID index_id, TupleData tuple_data)
{
    IndexKey index_key = 0;

    ColumnData column_data;

    void* index_attr[5];

    switch (index_id)
    {
    case A_PK_INDEX:
        {
            /** A_NAME **/
            g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_NAME, tuple_data, column_data);
            uint64_t a_name = *(uint64_t *)column_data;
            
            index_attr[0] = &a_name;
            index_key     = GetIndexKey(A_PK_INDEX, index_attr);
            break;
        }
    case S_PK_INDEX:
        {
            /** S_CUSTOMER_ID **/
            g_schema->GetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_CUSTOMER_ID, tuple_data, column_data);
            uint64_t s_customer_id = *(uint64_t *)column_data;

            index_attr[0] = &s_customer_id;
            index_key     = GetIndexKey(S_PK_INDEX, index_attr);

            break;
        }
    case C_PK_INDEX:
        {
            /** C_CUSTOMER_ID **/
            g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_CUSTOMER_ID, tuple_data, column_data);
            uint64_t c_customer_id = *(uint64_t *)column_data;

            index_attr[0] = &c_customer_id;
            index_key     = GetIndexKey(S_PK_INDEX, index_attr);
            break;
        }
    default:
        printf("SmallBankSchema::GetIndexKey: undefined index type in smallbank!\n");
        exit(0);
        break;
    }

    return index_key;
}


uint64_t SmallBankSchema::FetchIncID(TableID table_id)
{
    TableCatalog* table_catalog = table_catalogs_[table_id];
    return ATOM_FETCH_ADD(table_catalog->inc_id_, 1);
}


void SmallBankSchema::PrintTableCatalog()
{
    for (uint64_t i = 0; i < g_smallbank_table_num; i++)
    {
        if (table_catalogs_[i] == nullptr)
        {
            printf("null\n");
        }

        printf("table id: %d, tuple num: %ld, tuple size: %ld, col num: %ld \n", table_catalogs_[i]->table_id_, table_catalogs_[i]->tuple_num_, table_catalogs_[i]->tuple_size_, table_catalogs_[i]->col_num_);
    }
}


#endif