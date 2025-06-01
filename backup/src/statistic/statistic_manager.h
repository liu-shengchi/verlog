#ifndef STATISTIC_MANAGER_H_
#define STATISTIC_MANAGER_H_

#include "config.h"


class ClientStatistic;
class ReplayThreadStatistic;
class ExecuteThreadStatistic;
class ReclaimThreadStatistic;

class StatisticManager
{
private:

    ClientStatistic*        client_stats_[g_client_total_num];

    ReplayThreadStatistic*  replay_thread_stats_[g_replay_thread_num];

    ExecuteThreadStatistic* execute_thread_stats_[g_txn_thread_num];

    ReclaimThreadStatistic* reclaim_thread_stats_[g_reclaim_thread_num];


public:
    StatisticManager();
    ~StatisticManager();

    void PrintStatResult();

    void Reset();
    
    /* 
     * 系统处于testing状态的时间
     */
    ClockTime start_time_;
    ClockTime end_time_;


    void ClientStart(ClientID client_id);
    void ClientEnd(ClientID client_id);
    void TxnCommit(ClientID client_id);
    void TxnAbort(ClientID client_id);

    void FetchRTSTime(ClientID client_id, ClockTime fetch_rts_time);
    void WaitSCRSnapshot(ClientID client_id, ClockTime wait_time);
    void ExecuteTxnTime(ClientID client_id, ClockTime execute_time);

    void VisibilityGap(ClientID client_id, Timestamp gap);


    void TimeBreakdownStatc(ThreadID thread_id, DBThreadType thread_type, TimeBreakdownType time_type, ClockTime time);
  
    void MicroParameterStatc(ThreadID thread_id, DBThreadType thread_type, MicronStatisticType micro_type, uint64_t count);

};


#endif