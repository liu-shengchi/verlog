#include "config.h"
#include "global.h"

#include "util_function.h"

#include "ycsb_schema.h"
#include "tpcc_schema.h"

#include "tpcc_workload.h"
#include "ycsb_workload.h"

#include "thread_manager.h"
#include "statistic_manager.h"
#include "log_manager.h"

#include "log_buffer.h"

#include "comm_manager.h"
#include "system_control_comm.h"

#include "snapshot_manager.h"

#include "tuple_ver_chain_am.h"
#include "log_index_ver_chain_am.h"
#include "queryfresh_am.h"
#include "apply_writes_on_demand_am.h"

#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"



void InitSystem();
void ReceiveLog();
void TestCommunication();

void UpdateLogBufState();
void UpdateLogBufStateOnlyReceiveLog();


int main(void)
{
#if ONLY_RECEIVE_LOG
    ReceiveLog();
#else
    InitSystem();
#endif

    // TestCommunication();

    return 0;
}


void InitSystem()
{
    /****** initial global data structure ******/

    printf("备节点进入initial状态!\n\n");

    UtilFunc::InitUtilFunc();

    /* 初始化统计信息管理器 */
    g_statistic_manager = new StatisticManager();

    /* 初始化日志管理器 */
    g_log_manager = new LogManager();

    /* 初始化线程管理器 */
    g_thread_manager = new ThreadManager();


    //初始化访问策略
#if   AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
    g_am_strategy_ = new TupleVerChainAM();
#elif AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    g_am_strategy_ = new LogIndexVerChainAM();
#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    g_am_strategy_ = new QueryFreshAM();
#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    g_am_strategy_ = new ApplyWritesOnDemandAM();
#endif

    //初始化日志策略
#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    g_log_strategy_ = new SerialLog();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    g_log_strategy_ = new TaurusLog();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    g_log_strategy_ = new LogIndexLog();
#endif

    /* 创建schema, workload */
#if    WORKLOAD_TYPE == TPCC_W
    g_schema   = new TPCCSchema();
    g_workload = new TPCCWorkload();
#elif  WORKLOAD_TYPE == YCSB_W
    g_schema   = new YCSBSchema();
    g_workload = new YCSBWorkload();
#endif



    /* 初始化回放线程 */
    g_thread_manager->CreateReplayThread();
    /* 初始化日志持久化线程 */
    g_thread_manager->CreateLoggerThread();



    /* 初始化通信管理器 */
    g_comm_manager = new CommManager();
    g_comm_manager->ConnectWithRemote(); //与主节点建立连接

    
    /* 初始化日志复制线程 */
    g_thread_manager->CreateReplicateThread();

    SysControlComm* sys_control_comm = g_comm_manager->GetSysControlComm();

    while (g_system_state == SystemState::INITIAL_STATE)
    {
        sys_control_comm->ProcessRecvWC();
    }

    /* 初始化只读快照管理器 */
    g_snapshot_manager = new SnapshotManager();


    printf("备节点结束initial状态, 进入load状态!\n\n");



#if   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    /* 初始化回收线程 */
    g_thread_manager->CreateReclaimThread();
#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    /* 初始化应用线程 */
    g_thread_manager->CreateApplyThread();
#endif


    while (g_system_state == SystemState::LOADING_STATE)
    {
        sys_control_comm->ProcessRecvWC();
        UpdateLogBufState();
    }
    printf("备节点结束load状态, 进入warmup状态!\n\n");
    

    /* 创建事务线程，开始执行只读事务负载 */
    g_system_state = SystemState::WARMUP_STATE;
    // 创建事务负载线程
    g_thread_manager->CreateWorkloadThread();
    // 创建事务执行线程
    g_thread_manager->CreateTxnThread();

    while (g_system_state == SystemState::WARMUP_STATE) {
        sys_control_comm->ProcessRecvWC();
        UpdateLogBufState();
    }
    printf("备节点结束warmup状态, 进入testing状态!\n\n");
    
    
    GET_CLOCK_TIME(g_statistic_manager->start_time_);
    while (g_system_state == SystemState::TESTING_STATE) {
        sys_control_comm->ProcessRecvWC();

        UpdateLogBufState();
    }
    GET_CLOCK_TIME(g_statistic_manager->end_time_);


    printf("备节点结束testing状态, 进入cooldown状态!\n\n");
    while (g_system_state == SystemState::COOLDOWN_STATE) {
        sys_control_comm->ProcessRecvWC();

        UpdateLogBufState();

        // Timestamp min_visible_ts   = MAX_TIMESTAMP;
        // Timestamp min_reclaimed_ts = MAX_TIMESTAMP;
        
        // LogBuffer* log_buf = nullptr;
        // for (uint64_t i = 0; i < g_log_buffer_num; i++)
        // {
        //     log_buf = g_log_manager->GetLogBuffer(i);
        //     if (min_reclaimed_ts > log_buf->reclaimed_ts_)
        //     {
        //         min_reclaimed_ts = log_buf->reclaimed_ts_;
        //     }

        //     if (min_visible_ts > log_buf->replayed_commit_ts_)
        //     {
        //         min_visible_ts = log_buf->replayed_commit_ts_;
        //     }
        // }

        // g_visible_ts   = min_visible_ts;
        // g_reclaimed_ts = min_reclaimed_ts;


    }


    printf("备节点结束cooldown状态, 进入finish状态!\n\n");
    
    g_thread_manager->JoinTxnThread();
    g_thread_manager->JoinReplicateThread();
    g_thread_manager->JoinLoggerThread();
    g_thread_manager->JoinReplayThread();
    g_thread_manager->JoinWorkloadThread();
    g_thread_manager->JoinReclaimThread();

#if   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    g_thread_manager->JoinReclaimThread();
#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    g_thread_manager->JoinApplyThread();
#endif
 
    g_statistic_manager->PrintStatResult();

}


void UpdateLogBufState()
{

    Timestamp min_visible_ts   = MAX_TIMESTAMP;
    Timestamp min_reclaimed_ts = MAX_TIMESTAMP;
    
    LogBuffer* log_buf = nullptr;
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        log_buf = g_log_manager->GetLogBuffer(i);
        // if (min_reclaimed_ts > log_buf->reclaimed_ts_)
        // {
        //     min_reclaimed_ts = log_buf->reclaimed_ts_;
        // }

        if (min_visible_ts > log_buf->replayed_commit_ts_)
        {
            min_visible_ts = log_buf->replayed_commit_ts_;
        }

    #if     AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
        log_buf->free_lsn_ = 
                log_buf->replayed_lsn_ < log_buf->persistented_lsn_ ? log_buf->replayed_lsn_ : log_buf->persistented_lsn_;
    #elif   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
        log_buf->free_lsn_ = 
                log_buf->reclaimed_lsn_ < log_buf->persistented_lsn_ ? log_buf->reclaimed_lsn_ : log_buf->persistented_lsn_; 
    #elif   AM_STRATEGY_TYPE == QUERY_FRESH_AM
        uint64_t allow_free_lsn = 
                log_buf->replayed_lsn_ < log_buf->persistented_lsn_ ? log_buf->replayed_lsn_ : log_buf->persistented_lsn_;
    
        // if (allow_free_lsn > g_query_fresh_free_log_threshold)
        // {        
        //     log_buffer_->free_lsn_ = allow_free_lsn - g_query_fresh_free_log_threshold;
        // }

        if (allow_free_lsn > (log_buf->lsn_ - g_query_fresh_free_log_threshold))
        {
            log_buf->free_lsn_ = log_buf->lsn_ - g_query_fresh_free_log_threshold;
        }
    #elif   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
        uint64_t allow_free_lsn = 
                log_buf->replayed_lsn_ < log_buf->persistented_lsn_ ? log_buf->replayed_lsn_ : log_buf->persistented_lsn_;
    

        if (allow_free_lsn > (log_buf->lsn_ - g_on_demand_apply_write_log_threshold))
        {
            log_buf->free_lsn_ = log_buf->lsn_ - g_on_demand_apply_write_log_threshold;
        }
    
    #endif

    }

    g_visible_ts   = min_visible_ts;
    g_reclaimed_ts = min_reclaimed_ts;
}



void ReceiveLog()
{
    printf("备节点进入initial状态!\n\n");

    UtilFunc::InitUtilFunc();

//     /* 创建schema, workload */
// #if    WORKLOAD_TYPE == TPCC_W
//     g_schema   = new TPCCSchema();
//     g_workload = new TPCCWorkload();
// #elif  WORKLOAD_TYPE == YCSB_W
//     g_schema   = new YCSBSchema();
//     g_workload = new YCSBWorkload();
// #endif

//     //初始化访问策略
// #if   AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
//     g_am_strategy_ = new TupleVerChainAM();
// #elif AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
//     g_am_strategy_ = new LogIndexVerChainAM();
// #elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
//     g_am_strategy_ = new QueryFreshAM();
// #endif

//     //初始化日志策略
// #if   LOG_STRATEGY_TYPE == SERIAL_LOG
//     g_log_strategy_ = new SerialLog();
// #elif LOG_STRATEGY_TYPE == TAURUS_LOG
//     g_log_strategy_ = new TaurusLog();
// #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
//     g_log_strategy_ = new LogIndexLog();
// #endif

//     /* 初始化统计信息管理器 */
//     g_statistic_manager = new StatisticManager();

    /* 初始化日志管理器 */
    g_log_manager = new LogManager();

    /* 初始化线程管理器 */
    g_thread_manager = new ThreadManager();

    /* 初始化通信管理器 */
    g_comm_manager = new CommManager();
    g_comm_manager->ConnectWithRemote(); //与主节点建立连接

    SysControlComm* sys_control_comm = g_comm_manager->GetSysControlComm();

    while (g_system_state == SystemState::INITIAL_STATE)
    {
        sys_control_comm->ProcessRecvWC();
    }

    /* 初始化只读快照管理器 */
    // g_snapshot_manager = new SnapshotManager();


    printf("备节点结束initial状态, 进入load状态!\n\n");


    /* 初始化日志复制线程 */
    g_thread_manager->CreateReplicateThread();
    /* 初始化回放线程 */
    // g_thread_manager->CreateReplayThread();
    /* 初始化日志持久化线程 */
    g_thread_manager->CreateLoggerThread();
    /* 初始化回收线程 */
    // g_thread_manager->CreateReclaimThread();


    while (g_system_state == SystemState::LOADING_STATE)
    {
        sys_control_comm->ProcessRecvWC();
        UpdateLogBufStateOnlyReceiveLog();
    }
    printf("备节点结束load状态, 进入warmup状态!\n\n");
    

    /* 创建事务线程，开始执行只读事务负载 */
    g_system_state = SystemState::WARMUP_STATE;
    //创建事务负载线程、事务执行线程
    // g_thread_manager->CreateWorkloadThread();
    // g_thread_manager->CreateTxnThread();

    while (g_system_state == SystemState::WARMUP_STATE) {
        sys_control_comm->ProcessRecvWC();
        UpdateLogBufStateOnlyReceiveLog();
    }
    printf("备节点结束warmup状态, 进入testing状态!\n\n");
    
    
    // GET_CLOCK_TIME(g_statistic_manager->start_time_);
    while (g_system_state == SystemState::TESTING_STATE) {
        sys_control_comm->ProcessRecvWC();

        UpdateLogBufStateOnlyReceiveLog();
    }
    // GET_CLOCK_TIME(g_statistic_manager->end_time_);


    printf("备节点结束testing状态, 进入cooldown状态!\n\n");
    while (g_system_state == SystemState::COOLDOWN_STATE) {
        sys_control_comm->ProcessRecvWC();

        UpdateLogBufStateOnlyReceiveLog();
    }


    printf("备节点结束cooldown状态, 进入finish状态!\n\n");
    
    // g_thread_manager->JoinTxnThread();
    g_thread_manager->JoinReplicateThread();
    g_thread_manager->JoinLoggerThread();
    // g_thread_manager->JoinReplayThread();
    // g_thread_manager->JoinWorkloadThread();
    // g_thread_manager->JoinReclaimThread();

    // g_statistic_manager->PrintStatResult();
}



void UpdateLogBufStateOnlyReceiveLog()
{

    LogBuffer* log_buf = nullptr;
    for (uint64_t i = 0; i < g_log_buffer_num; i++)
    {
        log_buf = g_log_manager->GetLogBuffer(i);

        log_buf->free_lsn_ = log_buf->persistented_lsn_;
    }
}



void TestCommunication()
{
    /* 初始化日志管理器和日志缓冲区 */
    g_log_manager = new LogManager();

    /* 初始化通信管理器 */
    g_comm_manager = new CommManager();

    g_comm_manager->ConnectWithRemote();
    
}





