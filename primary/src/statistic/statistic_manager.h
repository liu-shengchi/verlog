#ifndef STATISTIC_MANAGER_H_
#define STATISTIC_MANAGER_H_

#include "config.h"


class ThreadStatistic;


class StatisticManager
{
private:

    ThreadStatistic** thread_stats_;

public:
    StatisticManager();
    ~StatisticManager();

    void PrintStatResult();

    void Reset();

    void ThreadStart(ThreadID thread_id);
    void ThreadEnd(ThreadID thread_id);
    void TxnCommit(ThreadID thread_id, uint64_t count);
    void TxnAbort(ThreadID thread_id, uint64_t count);

    void FetchLSNFail();
    
    //测试事务吞吐开始时间
    ClockTime start_time_;
    //测试事务吞吐结束时间
    ClockTime end_time_;
    

    uint64_t atom_fetch_lsn_fail_ = 0;
};


#endif