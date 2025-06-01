#ifndef BENCHMARK_YCSB_CONFIG_H_
#define BENCHMARK_YCSB_CONFIG_H_


#if   WORKLOAD_TYPE == YCSB_W


//YCSB scale
#define YCSB_RECORD_COUNT    g_ycsb_record_num
#define YCSB_FIELD_COUNT     10


/********** YCSB Schema **********/

#define YCSB_SCHEMA_PATH  "./../src/benchmark/ycsb/YCSB_schema.txt"

#define YCSB_TABLE_NUM  1
#define YCSB_INDEX_NUM  1

enum YCSBTableType
{
    YCSB_TABLE = 0
};

enum YCSBIndexType{
    YCSB_INDEX = 0
};

enum YCSBTxnType
{
    YCSB_TXN = 0
};

enum YCSBTableCol
{
    YCSB_KEY = 0,   // 用于主键的列，在schema文件中不包含
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


/************ IndexKey  ***********/
/***** MAIN_TABLE*****/
// 400w，需要超过g_ycsb_record_num
#define MT_ID_BITS  22

#define MT_ALL_BITS MT_ID_BITS


#endif
#endif