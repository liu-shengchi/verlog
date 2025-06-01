#ifndef BENCHMARK_SMALLBANK_CONFIG_H_
#define BENCHMARK_SMALLBANK_CONFIG_H_

#include "config.h"
#include "global.h"



#if   WORKLOAD_TYPE == SMALLBANK_W


/**************************************/
/*********** SMALLBANK Load ***********/
/**************************************/

constexpr uint64_t g_smallbank_shard_num              = 24;
constexpr uint64_t g_smallbank_account_num_per_shard  = 1000000;
constexpr double   g_smallbank_hot_account = 0.01;    
constexpr double   g_smallbank_skewness    = 0.99; //access hot accounts




/*****************************************/
/********* SMALLBANK Transaction *********/
/*****************************************/

enum SMALLBANKTxnType
{
    BALANCE_TXN = 0,
    DEPOSIT_CHECKING_TXN,
    TRANSACT_SAVING_TXN,
    AMALGAMATE_TXN,
    WRITE_CHECK_TXN
};


/** SMALLBANK各种事务比例 **/
constexpr uint64_t g_txn_total_ratio        = 1000;
constexpr uint64_t g_balance_ratio          = 200;
constexpr uint64_t g_deposit_checking_ratio = 400;
constexpr uint64_t g_transact_saving_ratio  = 600;
constexpr uint64_t g_amalgamate_ratio       = 800;
constexpr uint64_t g_write_check_ratio      = 1000;



/***************************************/
/*********** SMALLBANK Schema **********/
/***************************************/

#define SMALLBANK_SCHEMA_PATH   "./../src/benchmark/smallbank/SMALLBANK_schema.txt"


/********** SMALLBANK Table **********/

constexpr uint64_t g_smallbank_table_num = 3;

enum SMALLBANKTableType
{
    ACCOUNT_T = 0,
    SAVING_T,
    CHECKING_T
};


// table -> column
enum AccountCol
{
    A_NAME = 0,
	A_CUSTOMER_ID
};

enum SavingCol
{
    S_CUSTOMER_ID = 0,
	S_BALANCE
};

enum CheckingCol
{
    C_CUSTOMER_ID = 0,
	C_BALANCE
};




/********** SMALLBANK Index **********/

constexpr uint64_t g_smallbank_index_num = 3;

//目前没有实现tableid和主键索引之间的映射
//因此要保证表ID和该表对应的主键索引ID顺序一致
enum SMALLBANKIndexType
{
	A_PK_INDEX = 0,
	S_PK_INDEX,
	C_PK_INDEX
};


/***** ACCOUNT *****/
//128000 大于 g_smallbank_account_num_per_shard (100000)
#define A_NAME_BITS  17 

#define A_ALL_BITS A_NAME_BITS


/***** Saving *****/
//128000 大于 g_smallbank_account_num_per_shard (100000)
#define S_CUSTOMER_ID_BITS  17 

#define S_ALL_BITS S_CUSTOMER_ID_BITS


/***** Checking *****/
//128000 大于 g_smallbank_account_num_per_shard (100000)
#define C_CUSTOMER_ID_BITS  17 

#define C_ALL_BITS C_CUSTOMER_ID_BITS




#endif
#endif