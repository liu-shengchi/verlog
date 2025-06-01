#ifndef SYSTEM_CONFIG_H_
#define SYSTEM_CONFIG_H_

#include "global.h"

#include "tpcc_config.h"
#include "ycsb_config.h"

#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <iostream>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>

using namespace std;

/************************************************/
// variable type definition
/************************************************/

typedef uint64_t TableID;
typedef uint64_t ColumnID;
typedef uint64_t ShardID;
typedef uint64_t IndexID;

typedef uint64_t ThreadID;
typedef uint64_t TxnID;

typedef uint64_t ClientID;

typedef uint64_t   LogBufID;
typedef uint64_t   LogLSN;
typedef uint64_t   LogSize;
#define MAX_LOGLSN 0xffffffffffffffffU


typedef uint64_t LogLogicalPtr;
#define LOGBUF_MASK   0Xff00000000000000U
#define LOGBUF_OFFSET 56
#define LOGLSN_MASK   0X00ffffffffffffffU
#define LOGLSN_OFFSET 0


typedef uint64_t MemPtr;

typedef uint64_t PartitionKey;
typedef uint64_t IndexKey;
typedef uint64_t PrimaryKey;

typedef uint64_t Timestamp;
#define MAX_TIMESTAMP   0xffffffffffffffffU

typedef uint64_t ClockTime;
#define MAX_CLOCKTIME   0xffffffffffffffffU

typedef uint64_t ProcID;


typedef char* TupleData;
typedef char* ColumnData;



/***********************************/
// system
/***********************************/
#define PRIMARY_TYPE    1
#define BACKUP_TYPE     2

#define COMPUTER_NODE_TYPE BACKUP_TYPE

#define ONLY_RECEIVE_LOG  false


/***********************************/
// server hardware config
/***********************************/
#define NUMA_CNT               2
#define PHYSICAL_CORE_PER_NUMA 24
#define HYPER_THREAD           false

#define CACHE_LINE_SIZE        64



/*************************************************/
/******************* Read config *****************/
/*************************************************/

#define LLT_OPTIMATION            true

#define STRONGLY_CONSISTENT_READ  true



/*************************************************/
/********** workload config information **********/
/*************************************************/

/****** workload type *******/
#define TPCC_W  1
#define YCSB_W  2

#define WORKLOAD_TYPE        TPCC_W


#define SIMULATE_WORKLOAD    false


/************************************************/
/********** storage config information **********/
/************************************************/

/****** storage Strategy ******/
#define NO_SHARD_SS             1
#define SHARD_BY_WAREHOUSE_SS   2
#define SHARD_BY_YCSB_SHARD_SS  3

#define STORAGE_STRATEGY_TYPE   SHARD_BY_WAREHOUSE_SS


#define MAX_WAREHOUSE_CNT 100




/************************************/
/********** Access Method ***********/
/************************************/
#define TUPLE_VERSION_CHAIN_AM      1
#define LOG_INDEX_VERSION_CHAIN_AM  2   //VerLog
#define QUERY_FRESH_AM              3
#define ON_DEMAND_APPLY_WRITES_AM   4
#define FULL_APPLY_WRITES_AM        5


#define AM_STRATEGY_TYPE            LOG_INDEX_VERSION_CHAIN_AM


#define QUERY_FRESH_REPLAY_ARRAY         true

#define QUERY_FRESH_VERSION_CHAIN_RETRY  true

#define APPLY_WRITE_DURING_EXECUTING     true

#define OPTIMISTIC_READ_IN_INDEXLOG      false



/* 
 * 是否进行TimeBreakdown采样分析
 */
#define TIME_BREAKDOWN_ANALYTIC          true

/*
 * TimeBreakdown采样的时间类型
 */
enum TimeBreakdownType
{
    /* 
     * 回放线程相关的统计参数
     */

    REPLAY_WRITE_TXN_TIME_T = 0,
    REPLAY_IDLE_TIME_T,

    REPLAY_LOAD_ANALYSIS_LOG_TIME_T,
    REPLAY_PAYLOAD_TIME_T,
    REPLAY_OTHER_TIME_T,


    EALA_REPLAY_ACCESS_KV_INDEX_TIME_T,


    OA_REPLAY_RECORD_WRITE_INFO_TIME_T,
    
    FA_REPLAY_CREATE_VERSION_TIME_T,
    VL_MAINTAIN_LOG_CHAIN_TIME_T,


    EXECUTE_READ_TXN_TIME_T,
    EXECUTE_READ_REQUEST_TIME_T,
    EXECUTE_TXN_PAYLOAD_TIME_T, 

    OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T, 
    OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T,
    OA_EXECUTE_CREATE_VERSION_TIME_T,

    VL_RECLAIM_WRITE_TXN_TIME_T,
    VL_RECLAIM_IDLE_TIME_T
};


/*** 启用 Time Breakdown 统计参数 ***/
#define   USING_REPLAY_WRITE_TXN_TIME_T           true
#define   USING_REPLAY_IDLE_TIME_T                true

#define   USING_REPLAY_LOAD_ANALYSIS_LOG_TIME_T   true
#define   USING_REPLAY_PAYLOAD_TIME_T             true
#define   USING_REPLAY_OTHER_TIME_T               true

#define   USING_EALA_REPLAY_ACCESS_KV_INDEX_TIME_T  true
#define   USING_OA_REPLAY_RECORD_WRITE_INFO_TIME_T  true
#define   USING_FA_REPLAY_CREATE_VERSION_TIME_T     true
#define   USING_VL_MAINTAIN_LOG_CHAIN_TIME_T        true


#define   USING_EXECUTE_READ_TXN_TIME_T       true

#define   USING_EXECUTE_READ_REQUEST_TIME_T   true
#define   USING_EXECUTE_TXN_PAYLOAD_TIME_T    true

#define   USING_OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T  true 
#define   USING_OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T     true
#define   USING_OA_EXECUTE_CREATE_VERSION_TIME_T               true


#define   USING_VL_RECLAIM_WRITE_TXN_TIME_T         true
#define   USING_VL_RECLAIM_IDLE_TIME_T              true




#define MICRO_STATISTIC_ANALYTIC      true

/*
 * MicronStatistic采样的时间类型
 */
enum MicronStatisticType
{
    REPLAY_WRITE_TXN_COUNT_T = 0,

    EA_REPLAY_ACCESS_KV_INDEX_COUNT_T,
    
    EA_REPLAY_CREATE_VERSION_COUNT_T,

    LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T,
    
    LA_REPLAY_ACCESS_KV_INDEX_COUNT_T,
    
    VL_REPLAY_ACCESS_KV_INDEX_COUNT_T,
    VL_FAST_REPLAY_PATH_COUNT_T,


    EXECUTE_READ_TXN_COUNT_T,

    LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T,

    LA_EXECUTE_LOAD_LOG_COUNT_T,
    
    LA_EXECUTE_ACCESS_DISK_COUNT_T,
    
    // for Lazy-Apply (step4)
    LA_EXECUTE_CREATE_VERSION_COUNT_T,


    RECLAIM_WRITE_TXN_COUNT_T,
    VL_INVISIBLE_VERSION_COUNT_T,
    VL_MATERIAL_VERSION_COUNT_T
};

#define   USING_EA_REPLAY_ACCESS_KV_INDEX_COUNT_T               true
#define   USING_EA_REPLAY_CREATE_VERSION_COUNT_T                true
#define   USING_LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T   true
#define   USING_LA_REPLAY_ACCESS_KV_INDEX_COUNT_T               true
#define   USING_VL_REPLAY_CREATE_VERSION_COUNT_T                true
#define   USING_VL_FAST_REPLAY_PATH_COUNT_T                     true

#define   USING_LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T   true
#define   USING_LA_EXECUTE_LOAD_LOG_COUNT_T                     true
#define   USING_LA_EXECUTE_ACCESS_DISK_COUNT_T                  true
#define   USING_LA_EXECUTE_CREATE_VERSION_COUNT_T               true





/***********************************************/
/********** thread config information **********/
/***********************************************/

/*
 * 工作线程的类型，每种工作线程用于执行不同的任务
 * 1. INITIAL_THREAD_T
 *     初始化各种数据结构，例如各种全局管理器、负载schema和索引、事务访问元数据、日志缓冲区等
 * 2. TXN_THREAD_T
 *     执行事务，实现最关键的事务功能
 * 3. REPLAY_THREAD_T
 *     回放日志线程
 * 4. LOGGER_THREAD_T
 *     持久化日志，周期地将日志缓冲区中的日志持久化到持久设备
 * 5. REPLICATE_THREAD_T
 *     复制线程，接收来自主节点的日志，同步已回收的日志
 * 6. WORKLOAD_THREAD_T
 *     负载线程，生成待执行的事务，将事务放置在事务线程等待队列中
 */
enum DBThreadType
{
    LOAD_THREAD_T = 0,
    REPLAY_THREAD_T,
    TXN_THREAD_T,
    APPLY_WRITE_THREAD_T,
    RECLAIM_THREAD_T,
    LOGGER_THREAD_T,
    REPLICATE_THREAD_T,
    WORKLOAD_THREAD_T
};


#define THREAD_BIND_CORE    true
#define THREAD_BIND_SHARD   false




/*****************************************************/
/************* client config information *************/
/*****************************************************/

enum ClientStateType
{
    CLIENT_FREE = 0,
    CLIENT_WAITING,
    CLIENT_BUSY,
};




/*****************************************************/
/********** transaction config information ***********/
/*****************************************************/

#define THREAD_BIND_CLIENT  false



/*****************************************************/
/********** transaction config information ***********/
/*****************************************************/

/******** transction information ********/

enum RC {
    RC_OK,
    RC_NULL,
    RC_ERROR,
    RC_WAIT,
    RC_COMMIT,
    RC_ABORT
};


/*** 已使用，用于TxnContext标识事务不同阶段的执行状态 ***/
enum TXN_STATE {
    TXN_NULL,
    TXN_BEGIN,
    TXN_WAITING,
    TXN_RUNNING,
    TXN_COMMITED,
    TXN_ABORTED
};


enum AccessType {
    READ_AT = 0,
    UPDATE_AT,
    INSERT_AT,
    DELETE_AT
};


//max number of tuples that transaction accesses
#define MAX_TUPLE_ACCESS_PER_TXN    512
#define MAX_WRITE_ACCESS_PER_TXN    256



/********** concurrency control  **********/

#define  SILO_CC            1
#define  MVTO_CC            2
#define  S2PL_WAITDIE_CC    3

#define CC_STRATEGY_TYPE    S2PL_WAITDIE_CC




/************************************************/
/************ Log Config Information ************/
/************************************************/

/*
 * 是否启用日志
 * false 表示启用
 * true  表示关闭日志
 */
#define NO_LOG             false

//logger线程是否开启持久化日志功能
#define PERSIST_LOG        true

#define BATCH_PERSIST      false

/* 
 * 是否启用内存日志模式
 * true  表示日志采用内存状态，仅改变日志持久化相关的元数据信息，并不真正持久化
 * false 表示需要将日志持久到存储设备
 */
#define MEM_LOG            false


//与主节点同步已经回收的日志缓冲区空间，避免覆盖备节点日志缓冲区中还未处理的日志
#define SYN_RECLAIMED_LOG  true


//事务执行线程是否与某个日志缓冲区绑定
#define TXN_THREAD_BIND_LOG_BUFFER    true

//当前日志策略是否是分布式日志，即一个事务生成的日志，分布在多个日志流中
//TODO：目前系统不支持分布式日志
#define DISTRIBUTED_LOG               false


#define STRONGLY_CONSISTENCY_READ_ON_BACKUP  true



//预提交策略需要消耗CPU和内存资源，对事务吞吐率的影响在5%左右
#define PRE_COMMIT_TXN     true


/****************** Log Strategy ******************/
#define SERIAL_LOG         1
#define TAURUS_LOG         2
#define GSN_LOCAL_LOG      3
#define LOGINDEX_LOG       4
#define GSN_DIST_LOG       5

#define LOG_STRATEGY_TYPE  LOGINDEX_LOG


//单个事务生成的日志最大长度
#define MAX_TXN_LOG_SIZE    (1 << 15)     //32KB
#define MAX_TUPLE_LOG_SIZE  (1 << 10)     //1KB

#define IO_UNIT_SIZE        4096




/******* Data Type ********/

//data type
enum DataType
{
    INT64_DT,
    UINT64_DT,
    DOUBLE_DT,
    DATE_DT,      //year-month-day
    STRING_DT
};



/******* Index Config ********/

//Index Type
enum IndexType
{
    HASH_INDEX,
    BPLUS_TREE_INDEX
};

#define INDEX_TYPE HASH_INDEX


#define HASH_FILL_FACTOR  1

#define MAX_INDEX_MODIFY_PER_LOG  2



/******** Tuple Config ********/

#define MAX_TUPLE_BITS  10    //1kb
#define MAX_TUPLE_SIZE  (1 << MAX_TUPLE_BITS)




/**********************************/
/************** UTIL **************/
/**********************************/

#define RW_LOCK_PTHREAD   1
#define RW_LOCK_ATOMIC    2    //自己设计的读写锁

#define RW_LOCK_TYPE      RW_LOCK_ATOMIC


#define SPIN_LOCK_PTHREAD 1
#define SPIN_LOCK_LATCH   2    //自己设计的latch自旋锁

#define SPIN_LOCK_TYPE    SPIN_LOCK_LATCH


#endif
