#ifndef BENCHMARK_TPCC_CONFIG_H_
#define BENCHMARK_TPCC_CONFIG_H_

#include "global.h"

#include <stdint.h>


#if   WORKLOAD_TYPE == TPCC_W



/********** TPCC Transaction **********/

enum TPCCTxnType
{
    NEW_ORDER_TXN = 0,
    PAYMENT_TXN,
    DELIVERY_TXN,
    ORDER_STATUS_TXN,
    STOCK_LEVEL_TXN,
	STOCK_LEVEL_IN_WH_TXN
};

/** TPCC各种事务比例 **/
constexpr uint64_t TXN_TOTAL_RATIO      = 1000;
constexpr uint64_t ORDERSTATUS_RATIO    = 500;
constexpr uint64_t STOCKLEVEL_RATIO     = 1000;
// constexpr uint64_t STOCKLEVELINWH_RATIO = 1000;


constexpr uint64_t g_stock_level_scan_order_num = 20;


#define TPCC_BY_LAST_NAME_RATIO 0

#define TPCC_INSERT             true

constexpr uint64_t g_tpcc_scan_num = 100000;


/************ IndexKey  ***********/

/***** HISTORY *****/
//1600w，满足1600w个payment事务需要
#define H_ID_BITS   24

#define H_ALL_BITS  24


/***** Item *****/
//128000，大于100000
#define I_ID_BITS  17    //TODO：14的时候会出现访问STOCK错误，待查

#define I_ALL_BITS I_ID_BITS


/***** Warehouse *****/
//32 >= g_warehouse_num
#define W_ID_BITS    5

#define W_ALL_BITS   W_ID_BITS


/***** District *****/
//16，大于10
#define D_ID_BITS      4

#define D_W_ID_OFFSET  D_ID_BITS
#define D_W_ID_BITS    W_ID_BITS

#define D_ALL_BITS     D_ID_BITS + D_W_ID_BITS


/***** Customer *****/
//4096，大于3000
#define C_ID_BITS      12

#define C_D_ID_OFFSET  C_ID_BITS
#define C_D_ID_BITS    D_ID_BITS

#define C_W_ID_OFFSET  C_D_ID_OFFSET + C_D_ID_BITS
#define C_W_ID_BITS    W_ID_BITS

#define C_ALL_BITS     C_W_ID_OFFSET + C_W_ID_BITS


/***** Order *****/
//6.5w，保证每个District小于6.5w个order，indexkey编码不会重复
#define O_ID_BITS      16

#define O_D_ID_OFFSET  O_ID_BITS
#define O_D_ID_BITS    D_ID_BITS

#define O_W_ID_OFFSET  O_D_ID_OFFSET + O_D_ID_BITS
#define O_W_ID_BITS    W_ID_BITS

#define O_ALL_BITS     O_W_ID_OFFSET + O_W_ID_BITS


/***** Order-Cust *****/
#define O_C_O_ID_BITS     O_ID_BITS

#define O_C_C_ID_OFFSET   O_C_O_ID_BITS
#define O_C_C_ID_BITS     C_ID_BITS

#define O_C_D_ID_OFFSET O_C_C_ID_OFFSET + O_C_C_ID_BITS
#define O_C_D_ID_BITS   D_ID_BITS

#define O_C_W_ID_OFFSET O_C_D_ID_OFFSET + O_C_D_ID_BITS
#define O_C_W_ID_BITS   W_ID_BITS

#define O_C_ALL_BITS    O_C_W_ID_OFFSET + O_C_W_ID_BITS



/***** New Order *****/
//同Order
#define NO_ID_BITS      O_ID_BITS

#define NO_D_ID_OFFSET  NO_ID_BITS
#define NO_D_ID_BITS    D_ID_BITS

#define NO_W_ID_OFFSET  NO_D_ID_OFFSET + NO_D_ID_BITS
#define NO_W_ID_BITS    W_ID_BITS

#define NO_ALL_BITS     NO_W_ID_OFFSET + NO_W_ID_BITS


/***** Order-Line *****/
//16，每个order最多有15个order line
#define OL_NUMBER_BITS  4

#define OL_O_ID_OFFSET  OL_NUMBER_BITS        
#define OL_O_ID_BITS    O_ID_BITS

#define OL_D_ID_OFFSET  OL_O_ID_OFFSET + OL_O_ID_BITS
#define OL_D_ID_BITS    D_ID_BITS

#define OL_W_ID_OFFSET  OL_D_ID_OFFSET + OL_D_ID_BITS
#define OL_W_ID_BITS    W_ID_BITS

#define OL_ALL_BITS     OL_W_ID_OFFSET + OL_W_ID_BITS



/***** Stock *****/
#define S_I_ID_BITS   I_ID_BITS

#define S_W_ID_OFFSET S_I_ID_BITS 
#define S_W_ID_BITS   W_ID_BITS

#define S_ALL_BITS    S_I_ID_BITS + S_W_ID_BITS





/********** TPCC Schema **********/

#define TPCC_SCHEMA_PATH   "./../src/benchmark/tpcc/TPCC_full_schema.txt"

#define TPCC_TABLE_NUM  9

enum TPCCTableType
{
    WAREHOUSE_T = 0,
    DISTRICT_T,
    CUSTOMER_T,
	HISTORY_T,
	NEW_ORDER_T,
    ORDER_T,
    ORDER_LINE_T,
	STOCK_T,
	ITEM_T,
};


#define TPCC_INDEX_NUM 11

//目前没有实现tableid和主键索引之间的映射
//因此要保证表ID和该表对应的主键索引ID顺序一致
enum TPCCIndexType
{
	W_PK_INDEX = 0,
	D_PK_INDEX,
	C_PK_INDEX,
	H_PK_INDEX,
	NO_PK_INDEX,
	O_PK_INDEX,
	OL_PK_INDEX,
	S_PK_INDEX,
	I_PK_INDEX,
	C_LAST_INDEX,
	O_CUST_INDEX
};


enum WarehouseCol
{
    W_ID = 0,
	W_NAME,
	W_STREET_1,
	W_STREET_2,
	W_CITY,
	W_STATE,
	W_ZIP,
	W_TAX,
	W_YTD
};

enum DistrictCol
{
    D_ID = 0,
	D_W_ID,
	D_NAME,
	D_STREET_1,
	D_STREET_2,
	D_CITY,
	D_STATE,
	D_ZIP,
	D_TAX,
	D_YTD,
	D_NEXT_O_ID
};

enum CustomerCol
{
	C_ID = 0,
	C_D_ID,
	C_W_ID,
	C_FIRST,
	C_MIDDLE,
	C_LAST,
	C_STREET_1,
	C_STREET_2,
	C_CITY,
	C_STATE,
	C_ZIP,
	C_PHONE,
	C_SINCE,
	C_CREDIT,
	C_CREDIT_LIM,
	C_DISCOUNT,
	C_BALANCE,
	C_YTD_PAYMENT,
	C_PAYMENT_CNT,
	C_DELIVERY_CNT,
	C_DATA
};

enum OrderCol
{
	O_ID = 0,
	O_C_ID,
	O_D_ID,
	O_W_ID,
	O_ENTRY_D,
	O_CARRIER_ID,
	O_OL_CNT,
	O_ALL_LOCAL
};

enum NewOrderCol
{
	NO_O_ID = 0,
	NO_D_ID,
	NO_W_ID
};

enum HistoryCol
{
	H_ID = 0,
	H_C_ID,
	H_C_D_ID,
	H_C_W_ID,
	H_D_ID,
	H_W_ID,
	H_DATE,
	H_AMOUNT,
	H_DATA
};

enum OrderLineCol
{
    OL_O_ID = 0,
	OL_D_ID,
	OL_W_ID,
	OL_NUMBER,
	OL_I_ID,
	OL_SUPPLY_W_ID,
	OL_DELIVERY_D,
	OL_QUANTITY,
	OL_AMOUNT,
	OL_DIST_INFO
};

enum StockCol
{
	S_I_ID = 0,
	S_W_ID,
	S_QUANTITY,
	S_DIST_01,
	S_DIST_02,
	S_DIST_03,
	S_DIST_04,
	S_DIST_05,
	S_DIST_06,
	S_DIST_07,
	S_DIST_08,
	S_DIST_09,
	S_DIST_10,
	S_YTD,
	S_ORDER_CNT,
	S_REMOTE_CNT,
	S_DATA
};

enum ItemCol
{
	I_ID = 0,
	I_IM_ID,
	I_NAME,
	I_PRICE,
	I_DATA
};





/*********** TPCC Tuple Storage Information ***********/

// column offset in tuple for each attribute in table
#define W_ID_OFFSET 0



#endif
#endif