#ifndef SYSTEM_GLOBAL_H_
#define SYSTEM_GLOBAL_H_

#include "config.h"

#include <stdint.h>
#include <time.h>
#include <map>
#include <string>

class Schema;
class Workload;

class ThreadManager;

class TxnManager;
class StatisticManager;
class LogManager;
class CommManager;
class SnapshotManager;

class AMStrategy;
class LogStrategy;


/**************************************
 Global Variable:
 - Workload & Schema
 - Strategy
 - Manager
**************************************/

extern Workload*  g_workload;
extern Schema*    g_schema;

extern AMStrategy*  g_am_strategy_;
extern LogStrategy* g_log_strategy_;

extern ThreadManager*     g_thread_manager;
extern StatisticManager*  g_statistic_manager;
extern LogManager*        g_log_manager;
extern CommManager*       g_comm_manager;
extern SnapshotManager*   g_snapshot_manager;


/***********************************/
// system
/***********************************/

constexpr char     server_ip[] = "49.52.27.28";
constexpr uint64_t server_port = 5200;


constexpr char     primary_ip[] = "49.52.27.29";
constexpr uint64_t primary_port = 5000;


enum SystemState
{
	INITIAL_STATE = 0,
    LOADING_STATE,
    WARMUP_STATE,
	TESTING_STATE,
    COOLDOWN_STATE,
	FINISH_STATE
};


/* 
 * 备节点状态
 * 1. 备节点开始运行是处于LOADING_STATE状态，表示系统处于初始化WAREHOUSE的阶段，不运行只读事务。
 *    主节点Load线程初始化warehouse初始数据，并以日志形式发送到备节点，由备节点回放完成初始化。 
 * 2. 主节点LOAD阶段结束后，开始执行事务负载，向备节点发送WARMUP_STATE，备节点开始执行只读事务。
 *    主节点执行写事务，将事务日志复制到备节点，由备节点回放；备节点执行只读事务，向主节点获取CTS
 *    作为只读一致快照。
 * 3. 系统执行负载分为WARMUP、TEST、COOLDOWN三个阶段，为了更准确测出系统性能，只采样TEST阶段的系统性能数据。
 *    当主节点发送TESTING_STATE，备节点切换TESTING_STATE状态后，备节点才开始获取测试数据。当备节点接到
 *    COOLDOWN_STATE后，继续执行负载，但停止采集数据，
 * 4. 接收到FINISH_STATE状态，备节点结束执行负载。
 */
extern volatile SystemState g_system_state;




/***********************************/
// log
/***********************************/

constexpr uint64_t g_log_buffer_num  = 8;

extern const std::map<uint64_t, std::string> log_file_paths;

constexpr uint64_t g_log_buffer_size = ((0LU<<30) + (512LU<<20) + (0LU<<10)); 


constexpr uint64_t g_log_flush_interval = 0;


constexpr uint64_t g_trigger_reclaim_log_threshold = g_log_buffer_size * 0.80;

constexpr uint64_t g_query_fresh_free_log_threshold = g_log_buffer_size * 0.40;

constexpr uint64_t g_on_demand_apply_write_log_threshold = g_log_buffer_size * 0.80;


constexpr uint64_t g_reclaim_log_interval =  g_trigger_reclaim_log_threshold * 0.00625;  

constexpr uint64_t g_trigger_reclaim_log_threshold_in_loading = g_reclaim_log_interval * 2; 

constexpr uint64_t g_make_snapshot_interval = g_reclaim_log_interval * 1;

constexpr uint64_t g_syn_free_lsn_threshold = g_reclaim_log_interval * 1;



/***********************************/
// TPCC
/***********************************/

constexpr uint64_t g_warehouse_num = 24;
constexpr uint64_t g_dist_per_ware = 10;
constexpr uint64_t g_cust_per_dist = 3000;
constexpr uint64_t g_item_num      = 100000;




/***********************************/
// YCSB
/***********************************/
constexpr uint64_t g_ycsb_shard_num   = 24;
constexpr uint64_t g_ycsb_record_num  = 3350000;
constexpr uint64_t g_ycsb_scan_num    = 16;
constexpr double   g_ycsb_zipf_theta  = 1.20;




/***********************************/
// thread
/***********************************/
constexpr uint64_t g_replay_thread_num   = 10;
constexpr uint64_t g_reclaim_thread_num  = 2;  //小于等于g_log_buffer_num
constexpr uint64_t g_apply_thread_num    = 0;
constexpr uint64_t g_replica_thread_num  = 4;  //小于等于g_log_buffer_num
constexpr uint64_t g_txn_thread_num      = 10;
constexpr uint64_t g_logger_thread_num   = g_log_buffer_num;
constexpr uint64_t g_workload_thread_num = 1;
constexpr uint64_t g_db_thread_num       = g_replay_thread_num
										 + g_reclaim_thread_num
										 + g_replica_thread_num
										 + g_txn_thread_num
										 + g_logger_thread_num
										 + g_workload_thread_num;


constexpr uint64_t replay_thread_id_offset   = 0;
constexpr uint64_t reclaim_thread_id_offset  = g_replay_thread_num;
constexpr uint64_t apply_thread_id_offset    = g_replay_thread_num;
constexpr uint64_t replica_thread_id_offset  = g_replay_thread_num + g_reclaim_thread_num + g_apply_thread_num;
constexpr uint64_t txn_thread_id_offset      = g_replay_thread_num + g_replica_thread_num + g_reclaim_thread_num + 
											   g_apply_thread_num;
constexpr uint64_t logger_thread_id_offset   = g_replay_thread_num + g_replica_thread_num + g_reclaim_thread_num + 
											   g_apply_thread_num + g_txn_thread_num;

constexpr uint64_t workload_thread_id_offset = g_replay_thread_num + g_replica_thread_num + g_txn_thread_num + 
											   g_reclaim_thread_num + g_apply_thread_num + g_logger_thread_num;



constexpr uint64_t apply_write_array_length = 200;


constexpr uint64_t g_max_thread_num = 200;



/***********************************/
// client
/***********************************/

constexpr uint64_t g_client_num_per_txn_thread = 11;

constexpr uint64_t g_client_total_num          = g_txn_thread_num * g_client_num_per_txn_thread;
constexpr uint64_t g_max_client_num            = g_max_thread_num * g_client_num_per_txn_thread;



/***********************************/
// transaction
/***********************************/
//全局可见时间戳
extern volatile uint64_t g_visible_ts;

//主节点提交时间戳，通过g_commit_ts获取
extern volatile uint64_t g_commit_ts;

//已回收的事务日志
extern volatile uint64_t g_reclaimed_ts;



/* 向主节点获取g_commit_ts */
extern volatile uint64_t count1;     //直接返回 g_commit_ts 的次数
extern volatile uint64_t count2;     //等待后返回
extern volatile uint64_t count3;     //等待并向主节点获取 g_commit_ts


/* IndexLog日志回放 */
extern volatile uint64_t count4;     //prev日志已回收
extern volatile uint64_t count5;     //prev日志未回放（也等于重试的次数）
extern volatile uint64_t count6;     //无prev日志（Insert操作）
extern volatile uint64_t count7;     //prev日志已回放且未回收

/* IndexLog日志回收 */
extern volatile uint64_t count8;     //创建的元组版本数量

/* IndexLog遍历版本链 */
extern volatile uint64_t count9;     //可见版本在日志版本链
extern volatile uint64_t count10;    //可见版本在元组版本链


/* C5日志回放 */
extern volatile uint64_t count11;    //创建的元组版本数量 & 遍历索引的次数



/* QueryFresh创建元组版本链 */
extern volatile uint64_t count12;    //QueryFresh创建元组的数量
extern volatile uint64_t count13;    //QueryFresh访问磁盘的次数




/*****************************************/
// Time Breakdown 分析 变量
/*****************************************/

constexpr uint64_t       g_time_breakdown_sample_frequency = 1000;   
constexpr uint64_t       g_time_breakdown_max_sample_count = 100000;  

constexpr uint64_t       g_time_breakdown_distribution_proportion_gap   = 5;   
constexpr uint64_t       g_time_breakdown_distribution_proportion_start = 100; 
constexpr uint64_t       g_time_breakdown_distribution_proportion_end   = 101;





/*****************************************/
// Micro 分析 变量
/*****************************************/

constexpr uint64_t       g_micro_sample_frequency = 100;    //抽样频率






/* 每个事务执行结束后，事务线程会执行一个for循环，表示thinktime
 * g_think_time表示for循环的次数 */
constexpr uint64_t g_think_time = 5000;


constexpr uint64_t g_waiting_txn_queue_capacity = 2;




/***********************************/
// communication
/***********************************/
constexpr uint64_t g_qp_max_depth     = 200;
constexpr uint64_t g_max_message_size = 64;




/*************************************/
// get clock time
/*************************************/

#define GET_CLOCK_TIME(clock_time) {    \
		timespec * tp = new timespec;   \
		if (clock_gettime(CLOCK_REALTIME, tp) == -1)  \
		{                                             \
			printf("clock_gettime错误!\n");           \
			clock_time = 0;                           \
		}                                             \
		else                                          \
			clock_time = (uint64_t)(tp->tv_sec) * 1000000000 + tp->tv_nsec; \
	}

// #define GET_CLOCK_TIME(clock_time) {    \
// 	timespec * tp = new timespec;   \
// 	if (clock_gettime(CLOCK_MONOTONIC, tp) == -1)  \
// 	{                                             \
// 		printf("clock_gettime错误!\n");           \
// 		clock_time = 0;                           \
// 	}                                             \
// 	else                                          \
// 		clock_time = (uint64_t)(tp->tv_sec) * 1000000000 + tp->tv_nsec; \
// }


/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
	__sync_fetch_and_sub(&(dest), value)

// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
	__sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
	__sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
	__sync_sub_and_fetch(&(dest), value)

#define COMPILER_BARRIER { asm volatile("" ::: "memory"); }
#define PAUSE { __asm__ ( "pause;" ); }




#define likely(x)    __builtin_expect(!!(x), 1) 
#define unlikely(x)  __builtin_expect(!!(x), 0)




#endif