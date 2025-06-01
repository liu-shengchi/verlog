#ifndef SYSTEM_CONFIG_H_
#define SYSTEM_CONFIG_H_

#include "global.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>


/************************************************/
// variable type definition
/************************************************/

typedef uint64_t TableID;
typedef uint64_t ColumnID;
typedef uint64_t IndexID;
typedef uint64_t ShardID;

typedef uint64_t ThreadID;
typedef uint64_t TxnID;

typedef uint64_t LogBufID;
typedef uint64_t LogLSN;

typedef uint64_t MemPtr;

typedef uint64_t IndexKey;
typedef uint64_t PrimaryKey;

typedef uint64_t Timestamp;
#define MAX_TIMESTAMP   0xffffffffffffffff

typedef uint64_t ClockTime;
#define MAX_CLOCKTIME   0xffffffffffffffff

typedef uint64_t ProcID;

typedef char* TupleData;
typedef char* ColumnData;



/***********************************/
// system
/***********************************/
#define PRIMARY_TYPE    1
#define BACKUP_TYPE     2

#define COMPUTER_NODE_TYPE PRIMARY_TYPE

/*
 * 当前系统是stand-alone模式，还是primary-backup模式
 * false 表示stand-alone模式
 * true  表示primary-backup模式
 */
#define PRIMARY_BACKUP true




/***********************************/
// server hardware config
/***********************************/
#define NUMA_CNT               2
#define PHYSICAL_CORE_PER_NUMA 24
#define HYPER_THREAD           false

#define CACHE_LINE_SIZE        64



/*************************************************/
/********** workload config information **********/
/*************************************************/

/****** workload type *******/

#define TPCC_W       1
#define YCSB_W       2
#define SMALLBANK_W  3

#define WORKLOAD_TYPE TPCC_W



/************************************************/
/********** storage config information **********/
/************************************************/

/****** storage Strategy ******/
#define NO_SHARD_SS                   1
#define SHARD_BY_WAREHOUSE_SS         2
#define SHARD_BY_YCSB_SHARD_SS        3
#define SHARD_BY_SMALLBANK_ACCOUNT_SS 4


#define STORAGE_STRATEGY_TYPE  SHARD_BY_WAREHOUSE_SS




/***********************************************/
/********** thread config information **********/
/***********************************************/

/*
 * 工作线程的类型，每种工作线程用于执行不同的任务
 * 1. INITIAL_THREAD_T
 *     初始化各种数据结构，例如各种全局管理器、负载schema和索引、事务访问元数据、日志缓冲区等
 * 2. LOAD_THREAD_T
 *     加载负载初始数据，例如TPCC每个warehouse的初始数据
 * 3. TXN_THREAD_T
 *     执行事务，实现最关键的事务功能
 * 4. LOGGER_THREAD_T
 *     持久化日志，周期地将日志缓冲区中的日志持久化到持久设备
 */
enum DBThreadType
{
    INITIAL_THREAD_T = 0,
    LOAD_THREAD_T,
    TXN_THREAD_T,
    LOGGER_THREAD_T,
    REPLICATE_THREAD_T
};


#define THREAD_BIND_CORE     true
#define THREAD_BIND_SHARD    true



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


/*** 暂时不使用，事务状态的类别和转移逻辑需要深入研究定义 ***/
enum TXN_STATE {
    TXN_RUNNING,
    TXN_PRECOMMIT,
    TXN_ABORTED,
    TXN_COMMITED
};


enum AccessType {
    READ_AT = 0,
    UPDATE_AT,
    INSERT_AT,
    DELETE_AT
};


//max number of tuples that transaction accesses
#define MAX_TUPLE_ACCESS_PER_TXN    1024
#define MAX_WRITE_ACCESS_PER_TXN    256



/********** concurrency control  **********/

#define  SILO_CC            1
#define  MVTO_CC            2
#define  S2PL_WAITDIE_CC    3

#define CC_STRATEGY_TYPE    S2PL_WAITDIE_CC



/********* Distributed Transaction **********/

/* 
 * 是否启用分布式事务
 * 在TPCC负载中，使用分布式事务意味着允许事务访问多个warehouse，
 * 在当前实现中，TPCC负载每个warehouse一个shard，且每个每个分布式事务
 * 最多访问两个warehouse，不支持访问三个及以上warehouse。
 */
#define DISTRIBUTED_TXN    true

#define TWO_PHASE_COMMIT   false




/************************************************/
/************ Log Config Information ************/
/************************************************/
/*
 * 是否启用日志
 * false 表示启用日志，事务存在日志关键路径
 * true  表示关闭日志
 */
#define NO_LOG             false

//logger线程是否开启持久化日志功能
#define PERSIST_LOG        false

#define BATCH_PERSIST      true

/* 
 * 是否启用内存日志模式
 * true  表示日志采用内存状态，仅改变日志持久化相关的元数据信息，并不真正持久化
 * false 表示需要将日志持久到存储设备
 */
#define MEM_LOG            true

//是否将日志复制到备节点
#define REPLICATE_LOG      true

//Load阶段数据是否写入日志
#define LOAD_DATA_LOG      true

//与备节点同步已经回收的日志缓冲区空间，避免覆盖备节点日志缓冲区中还未处理的日志
#define SYN_RECLAIMED_LOG  true

//事务执行线程是否与某个日志缓冲区绑定
#define TXN_THREAD_BIND_LOG_BUFFER    true


//当前日志策略是否是分布式日志，即一个事务生成的日志，分布在多个日志流中
//TODO：目前系统不支持分布式日志
#define DISTRIBUTED_LOG               false

/* 日志是否支持备节点强一致读，当前实现是日志包含事务的COMMIT_TS */
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
#define MAX_TXN_LOG_SIZE    1 << 15     //32KB

#define IO_UNIT_SIZE        512



/** 
 * 日志中记录的日志类型
 * 1. TUPLE_DATA_LOG 日志记录整个元组的数据
 * 2. CHANGE_LOG     日志中记录元组在当前事务中相较上一元组版本变更的属性数据 **/
#define TUPLE_DATA_LOG     1
#define CHANGE_LOG         2

#define LOG_CONTENT_TYPE   1





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


#define HASH_FILL_FACTOR 1


/******** Tuple Config ********/

#define MAX_TUPLE_BITS  10     //(1kb)
#define MAX_TUPLE_SIZE  (1 << MAX_TUPLE_BITS)



/**********************************/
/************** UTIL **************/
/**********************************/

#define RW_LOCK_PTHREAD  1
#define RW_LOCK_LATCH    2
#define RW_LOCK_ATOMIC   3

#define RW_LOCK_TYPE     RW_LOCK_ATOMIC



#endif