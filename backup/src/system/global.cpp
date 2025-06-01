#include "global.h"

#include "schema.h"
#include "workload.h"

#include "thread_manager.h"
#include "statistic_manager.h"
#include "log_manager.h"
#include "comm_manager.h"
#include "snapshot_manager.h"

#include "am_strategy.h"

#include "log_strategy.h"


Workload*  g_workload = nullptr;
Schema*    g_schema   = nullptr;

AMStrategy*       g_am_strategy_  = nullptr;
LogStrategy*      g_log_strategy_ = nullptr;

ThreadManager*    g_thread_manager    = nullptr;
StatisticManager* g_statistic_manager = nullptr;
LogManager*       g_log_manager       = nullptr;
CommManager*      g_comm_manager      = nullptr;
SnapshotManager*  g_snapshot_manager  = nullptr;



/***********************************/
// system
/***********************************/
volatile SystemState g_system_state = SystemState::INITIAL_STATE;



/***********************************/
// transaction
/***********************************/
alignas(CACHE_LINE_SIZE) volatile Timestamp g_visible_ts   = 0;

alignas(CACHE_LINE_SIZE) volatile Timestamp g_commit_ts    = 0;

alignas(CACHE_LINE_SIZE) volatile Timestamp g_reclaimed_ts = 0;


const std::map<uint64_t, std::string> log_file_paths = 
                                    {{0, "/home/ssd0/log1.data"},
                                     {1, "/home/ssd1/log2.data"},
                                     {2, "/home/ssd2/log3.data"},
                                     {3, "/home/ssd3/log4.data"},
                                     {4, "/home/ssd0/log5.data"},
                                     {5, "/home/ssd1/log6.data"},
                                     {6, "/home/ssd2/log7.data"},
                                     {7, "/home/ssd3/log8.data"}};









/**** 微观统计信息 ****/

/* 向主节点获取g_commit_ts */
volatile uint64_t count1  = 0;     //直接返回 g_commit_ts 的次数
volatile uint64_t count2  = 0;     //等待后返回
volatile uint64_t count3  = 0;     //等待并向主节点获取 g_commit_ts



/* IndexLog日志回放 */
volatile uint64_t count4  = 0;     //prev日志已回收
volatile uint64_t count5  = 0;     //prev日志未回放（也等于重试的次数）
volatile uint64_t count6  = 0;     //无prev日志（Insert操作）
volatile uint64_t count7  = 0;     //prev日志已回放且未回收

/* IndexLog日志回收 */
volatile uint64_t count8  = 0;     //创建的元组版本数量

/* IndexLog遍历版本链 */
volatile uint64_t count9  = 0;     //可见版本在日志版本链
volatile uint64_t count10 = 0;     //可见版本在元组版本链



/* C5日志回放 */
volatile uint64_t count11 = 0;     //创建的元组版本数量 & 遍历索引的次数



/* QueryFresh创建元组版本链 */
volatile uint64_t count12 = 0;     //QueryFresh创建元组的数量
volatile uint64_t count13 = 0;     //QueryFresh访问磁盘的次数




// volatile uint64_t sample_frequency  = 100; // 100次抽样一次


volatile uint64_t fetch_analy_time  = 0;   // 获取 + 解析日志 用时
volatile uint64_t replay_log_time   = 0;   // 回放日志 用时（仅回放线程，在QueryFresh中不包括版本物化开销）
volatile uint64_t state_update_time = 0;   // 更新状态 用时

volatile uint64_t depen_track_time  = 0;   // 依赖跟踪用时
volatile uint64_t material_ver_time = 0;   // 物化版本用时
volatile uint64_t index_access_time = 0;   // 索引访问用时
volatile uint64_t install_ver_time  = 0;   // 插入版本链用时
volatile uint64_t other_time        = 0;   



volatile uint64_t rm_depen_track_time  = 0;   // 依赖跟踪用时
volatile uint64_t rm_material_ver_time = 0;   // 物化版本用时
volatile uint64_t rm_load_log_time     = 0;   // 获取日志用时
volatile uint64_t rm_install_ver_time  = 0;   // 插入版本链用时



volatile uint64_t il_depen_track_time = 0;    // 依赖跟踪用时
volatile uint64_t il_install_ver_time = 0;    // 安装版本用时
