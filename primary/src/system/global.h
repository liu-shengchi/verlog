#ifndef SYSTEM_GLOBAL_H_
#define SYSTEM_GLOBAL_H_

#include "config.h"

#include <stdint.h>
#include <time.h>
#include <map>
#include <string>


class Workload;
class Schema;

class ThreadManager;

class TxnManager;
class StatisticManager;
class LogManager;
class CommManager;

class CCStrategy;
class LogStrategy;


/**************************************/
//关键全局变量
/**************************************/

extern Workload*  g_workload;
extern Schema*    g_schema;

extern CCStrategy*        g_cc_strategy_;
extern LogStrategy*       g_log_strategy_;

extern ThreadManager*     g_thread_manager;
extern StatisticManager*  g_statistic_manager;
extern LogManager*        g_log_manager;
extern CommManager*       g_comm_manager;




/***********************************/
// system
/***********************************/

constexpr char     server_ip[] = "49.52.27.29";
constexpr uint64_t server_port = 5000;


constexpr    uint64_t                        g_backup_cnt = 1;
extern const std::map<uint64_t, std::string> backup_ip_map;
extern const std::map<uint64_t, uint32_t>    backup_port_map;


/** 系统状态 **/
enum SystemState
{
	INITIAL_STATE = 0,
    LOADING_STATE,
    WARMUP_STATE,
	TESTING_STATE,
    COOLDOWN_STATE,
	FINISH_STATE,
};

extern volatile SystemState g_system_state;




/***********************************/
// log
/***********************************/
/** 日志缓冲区的数量 **/
constexpr uint64_t g_log_buffer_num  = 8;

//日志文件路径
extern const std::map<uint64_t, std::string> log_file_paths;

//每个日志缓冲区大小 xGB + yMB + zKB
constexpr uint64_t g_log_buffer_size = ((0LU<<30) + (512LU<<20) + (0LU<<10)); 
//备节点日志缓冲区大小 xGB + yMB + zKB
constexpr uint64_t g_backup_log_buf_size = ((0LU<<30) + (512LU<<20) + (0LU<<10));

//日志刷盘时间间隔 单位us
constexpr uint64_t g_log_flush_interval    = 50;

//每次日志复制的最小日志数量阈值，单位 xMB + yKB
constexpr uint64_t g_log_replica_threshold = ((0LU<<20) + (0LU<<10));






/***********************************/
// Schema
/***********************************/

constexpr uint64_t g_max_sec_indx_per_table = 5;

constexpr uint64_t g_max_shard_num = 100;




/***********************************/
// thread
/***********************************/
constexpr uint64_t g_max_thread_num     = 200;

constexpr uint64_t g_txn_thread_num     = 24;
constexpr uint64_t g_logger_thread_num  = g_log_buffer_num;
constexpr uint64_t g_replica_thread_num = 4;
constexpr uint64_t g_db_thread_num      = g_txn_thread_num + g_logger_thread_num + g_replica_thread_num;

constexpr uint64_t txn_thread_id_offset     = 0;
constexpr uint64_t logger_thread_id_offset  = g_txn_thread_num;
constexpr uint64_t replica_thread_id_offset = g_txn_thread_num + g_logger_thread_num;


constexpr uint64_t g_load_thread_num    = g_txn_thread_num;



/* 
 * 目前数据库系统配置，一个district的order数量不能超过6.5w，一个warehouse的order数量不能超过65w
 * 当TPCC包含插入操作时，每个warehouse提交NewOrder事务数量不能超过65w
 * 如果不包含插入操作（TPCC_INSERT == false in tpcc_config.h），则无所谓。
 * 
 * 如果需要提高订单容量，需要修改 tpcc_config.h->O_ID_BITS 参数
 */
constexpr uint64_t g_warmup_txns_num_per_thread   =   50000;
constexpr uint64_t g_test_txns_num_per_thread     =  400000;
constexpr uint64_t g_cooldown_txns_num_per_thread =       0;
constexpr uint64_t g_txns_num_per_thread = g_warmup_txns_num_per_thread + 
										   g_test_txns_num_per_thread + 
										   g_cooldown_txns_num_per_thread;





/***********************************/
// transaction
/***********************************/
/* 全局提交时间戳，决定事务可串化顺序 */
extern uint64_t g_commit_ts;

/* 每个事务执行结束后，事务线程会执行一个for循环，表示thinktime
 * g_think_time表示for循环的次数 */
constexpr uint64_t g_think_time = 6000;


constexpr uint64_t g_load_think_time = 300;



/***********************************/
// communication
/***********************************/
constexpr uint64_t g_qp_max_depth     = 100;
constexpr uint64_t g_max_message_size = 64;




extern volatile uint64_t count1;
extern volatile uint64_t count2;
extern volatile uint64_t count3;
extern volatile uint64_t count4;




/*************************************/
// get clock time
/*************************************/

#define GET_CLOCK_TIME(clock_time) {  \
		timespec * tp = new timespec; \
		clock_gettime(CLOCK_REALTIME, tp); \
		clock_time = tp->tv_sec * 1000000000 + tp->tv_nsec; \
	}



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
#define PAUSE            { __asm__ ( "pause;" ); }




#endif
