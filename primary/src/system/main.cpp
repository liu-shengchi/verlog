#include "config.h"
#include "global.h"

#include "util_function.h"

#include "ycsb_schema.h"
#include "ycsb_workload.h"
#include "ycsb_util.h"

#include "tpcc_schema.h"
#include "tpcc_workload.h"
#include "tpcc_util.h"

#include "smallbank_schema.h"
#include "smallbank_workload.h"
#include "smallbank_util.h"

#include "thread_manager.h"

#include "s2pl_waitdie.h"

#include "statistic_manager.h"

#include "log_manager.h"
#include "log_buffer.h"
#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"

#include "comm_manager.h"
#include "system_control_comm.h"


void InitSystem();
void TestCommunication();
void SwichBackupSysState(SystemState system_state);


int main(void)
{
    InitSystem();
    
    // TestCommunication();

    return 0;
}


void InitSystem()
{
    /****** initial global data structure ******/

    //初始功能函数、随即种子
    UtilFunc::InitUtilFunc();

    /* 创建schema workload */
#if   WORKLOAD_TYPE == TPCC_W
    g_schema   = new TPCCSchema();
    g_workload = new TPCCWorkload();
    TPCCUtilFunc::InitTPCCUtilFunc();
#elif WORKLOAD_TYPE == YCSB_W
    g_schema   = new YCSBSchema();
    g_workload = new YCSBWorkload();
    YCSBUtilFunc::InitYCSBUtilFunc();
#elif WORKLOAD_TYPE == SMALLBANK_W
    g_schema   = new SmallBankSchema();
    g_workload = new SmallBankWorkload();
    SMALLBANKUtilFunc::InitSMALLBANKUtilFunc();
#endif


    //初始化并发控制策略
#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    g_cc_strategy_ = new S2plWDStrategy();
#endif

    //初始化日志策略
#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    g_log_strategy_ = new SerialLog();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    g_log_strategy_ = new TaurusLog();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    g_log_strategy_ = new LogIndexLog();
#endif

    /* 初始化统计信息管理器 */
    g_statistic_manager = new StatisticManager();

    /* 初始化日志管理器 */
    g_log_manager = new LogManager();

    /* 初始化线程管理器 */
    g_thread_manager = new ThreadManager();

#if PRIMARY_BACKUP
    /* 初始化通信管理器 */
    g_comm_manager = new CommManager();
    /* 与备节点建立通信连接 */
    g_comm_manager->ConnectWithRemote();
#endif


#if PRIMARY_BACKUP
    /* 创建replicate线程，向备节点复制日志 */
    g_thread_manager->CreateReplicateThread();
#endif

    /*
     * 将系统状态改为LOADING_STATE
     * 向备节点发送状态切换消息
     */
    g_system_state = SystemState::LOADING_STATE;
    printf("\n主节点进入load状态!\n");
#if PRIMARY_BACKUP
    SwichBackupSysState(g_system_state);
    printf("所有备节点进入load状态!\n\n");
#endif

    /* 创建logger线程，持久化日志 */
    g_thread_manager->CreateLoggerThread();

    /* 创建数据load线程，load数据 */
    g_thread_manager->CreateLoadThread();
    g_thread_manager->JoinLoadThread();

    printf("\n主节点完成数据load!\n\n");

    for (int i = 0; i < g_log_buffer_num; i++)
    {
        LogBuffer* log_buf = g_log_manager->GetLogBuffer(i);
        printf("logbufferid: %ld, log lsn: %ld\n", i, log_buf->lsn_);
    }


    /* 创建事务线程 */
    g_thread_manager->CreateTxnThread();    
    
    //等待事务线程初始化事务结束
    pthread_barrier_wait(g_thread_manager->execute_workload_barrier_);


    /*
     * 将系统状态改为WARMUP_STATE
     * 向备节点发送状态切换消息
     */
    g_system_state = SystemState::WARMUP_STATE;
    printf("主节点进入warmup状态!\n");
#if PRIMARY_BACKUP
    SwichBackupSysState(g_system_state);
    printf("所有备节点进入warmup状态!\n\n");
#endif

    while (!g_workload->warmup_finish_)
    {
        PAUSE
    }

    /*
     * 将系统状态改为TESTING_STATE
     * 向备节点发送状态切换消息
     */
    g_system_state = SystemState::TESTING_STATE;
    GET_CLOCK_TIME(g_statistic_manager->start_time_);
    printf("主节点warmup阶段结束, 进入testing状态!\n");
#if PRIMARY_BACKUP
    SwichBackupSysState(g_system_state);
    printf("所有备节点进入testing状态!\n\n");
#endif
    while (!g_workload->testing_finish_)
    {
        PAUSE
    }
    GET_CLOCK_TIME(g_statistic_manager->end_time_);

    /*
     * 将系统状态改为COOLDOWN_STATE
     * 向备节点发送状态切换消息
     */
    g_system_state = SystemState::COOLDOWN_STATE;
    printf("主节点testing阶段结束, 进入cooldown状态!\n\n");
#if PRIMARY_BACKUP
    SwichBackupSysState(g_system_state);
    printf("所有备节点进入cooldown状态!\n\n");
#endif
    while (!g_workload->workload_finish_)
    {
        PAUSE
    }


    /*
     * 将系统状态改为FINISH_STATE
     * 向备节点发送状态切换消息
     */
    g_system_state = SystemState::FINISH_STATE;
    printf("主节点结束cooldown状态, 进入finish状态!\n");
#if PRIMARY_BACKUP
    SwichBackupSysState(g_system_state);
    printf("所有备节点进入finish状态!\n\n");
#endif

    g_thread_manager->JoinTxnThread();
    g_thread_manager->JoinLoggerThread();
#if PRIMARY_BACKUP
    g_thread_manager->JoinReplicateThread();
#endif

    g_statistic_manager->PrintStatResult();

    sleep(0);
}



void TestCommunication()
{
    /* 初始化日志管理器和日志缓冲区 */
    g_log_manager = new LogManager();

    /* 初始化通信管理器 */
    g_comm_manager = new CommManager();
    
    g_comm_manager->ConnectWithRemote();
    
}


void SwichBackupSysState(SystemState system_state)
{
    SysControlComm* sys_control_comm = nullptr;

    for (uint64_t backup_id = 0; backup_id < g_backup_cnt; backup_id++)
    {
        sys_control_comm = g_comm_manager->GetSysControlComm(backup_id);
        sys_control_comm->SystemStateSwitch(g_system_state);
    }
    
    for (uint64_t backup_id = 0; backup_id < g_backup_cnt; backup_id++)
    {
        sys_control_comm = g_comm_manager->GetSysControlComm(backup_id);
        while (sys_control_comm->ProcessSendWC() != 1)
        {
            // printf("等待控制信息同步完成!\n");
            PAUSE
        }
    }
}