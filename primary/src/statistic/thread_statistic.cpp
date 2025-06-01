#include "thread_statistic.h"


ThreadStatistic::ThreadStatistic()
{
    thread_start_time_ = 0;
    thread_end_time_   = 0;
    txn_amount_ = 0;
    txn_commit_ = 0;
    txn_abort_  = 0;
}


void ThreadStatistic::ReSet()
{
    txn_amount_ = 0;
    txn_commit_ = 0;
    txn_abort_  = 0;
}
