#ifndef BENCHMARK_YCSB_CONFIG_H_
#define BENCHMARK_YCSB_CONFIG_H_

#include "config.h"
#include "global.h"


#if  WORKLOAD_TYPE  == YCSB_W

/**************************************/
/********** YCSB Transaction **********/
/**************************************/

/******** Parameter ********/

enum YCSBTxnType
{
    YCSB_TXN = 0
};

constexpr uint64_t g_ycsb_request_num_per_txn = 16;
constexpr uint64_t g_ycsb_rw_total_ratio      = 100;
constexpr uint64_t g_ycsb_read_ratio          = 0;
constexpr uint64_t g_ycsb_write_ratio         = 100;
constexpr uint64_t g_ycsb_remote_ratio        = 0.05;
constexpr uint64_t g_ycsb_remote_record_num   = 1;
constexpr double   g_ycsb_zipf_theta          = 1.20;


/******** Population ********/

constexpr uint64_t g_ycsb_shard_num            = 24;
constexpr uint64_t g_ycsb_record_num_per_shard = 3350000;
// constexpr uint64_t g_ycsb_all_record_num       = g_ycsb_shard_num * g_ycsb_record_num_per_shard;
constexpr uint64_t g_ycsb_field_num         = 10;
constexpr uint64_t g_ycsb_field_size        = 10;





/**************************************/
/************* YCSB Schema ************/
/**************************************/

#define YCSB_SCHEMA_PATH  "./../src/benchmark/ycsb/YCSB_schema.txt"


/********** Table *********/

constexpr uint64_t g_ycsb_table_num  = 1;

enum YCSBTableType
{
    YCSB_TABLE = 0
};

enum YCSBTableCol
{
    YCSB_KEY = 0,   // 用于主键的列
    F0,
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
};



/********** Index *********/

constexpr uint64_t g_ycsb_index_num  = 1;

enum YCSBIndexType{
    YCSB_INDEX = 0
};


/********** IndexKey  *********/
/***** YCSB_TABLE *****/
//大于400w (g_ycsb_record_num_per_shard)
#define YT_ID_BITS  22

#define YT_ALL_BITS YT_ID_BITS




#endif
#endif