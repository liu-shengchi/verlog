#include "global.h"

#include "schema.h"
#include "workload.h"

#include "thread_manager.h"
#include "statistic_manager.h"
#include "log_manager.h"

#include "cc_strategy.h"
#include "log_strategy.h"



/**************************************/
//关键全局变量
/**************************************/

Workload*  g_workload = nullptr;
Schema*    g_schema   = nullptr;

CCStrategy*  g_cc_strategy_  = nullptr;
LogStrategy* g_log_strategy_ = nullptr;

ThreadManager*    g_thread_manager    = nullptr;
LogManager*       g_log_manager       = nullptr;
CommManager*      g_comm_manager      = nullptr;
StatisticManager* g_statistic_manager = nullptr;



/***********************************/
// system
/***********************************/

const std::map<uint64_t, std::string> backup_ip_map   = {{0, "49.52.27.28"},
                                                         {1, "49.52.27.35"},
                                                         {2, "49.52.27.35"},
                                                         {3, "49.52.27.35"}};
const std::map<uint64_t, uint32_t>    backup_port_map = {{0, 5200},
                                                         {1, 5400},
                                                         {2, 5400},
                                                         {3, 5400}};


volatile SystemState g_system_state = SystemState::LOADING_STATE;


/***********************************/
// log
/***********************************/
const std::map<uint64_t, std::string> log_file_paths = 
                                    {{0, "/home/ssd0/log1.data"},
                                     {1, "/home/ssd1/log2.data"},
                                     {2, "/home/ssd2/log3.data"},
                                     {3, "/home/ssd3/log4.data"},
                                     {4, "/home/ssd0/log5.data"},
                                     {5, "/home/ssd1/log6.data"},
                                     {6, "/home/ssd2/log7.data"},
                                     {7, "/home/ssd3/log8.data"}};


/***********************************/
// TPCC
/***********************************/



/***********************************/
// thread
/***********************************/



/***********************************/
// transaction
/***********************************/
uint64_t g_commit_ts = 1;



/***********************************/
// communication
/***********************************/




volatile uint64_t count1  = 0;
volatile uint64_t count2  = 0;
volatile uint64_t count3  = 0;
volatile uint64_t count4  = 0;