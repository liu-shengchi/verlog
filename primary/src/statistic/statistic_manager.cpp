#include "statistic_manager.h"

#include "global.h"

#include "thread_statistic.h"

#include "stdio.h"


StatisticManager::StatisticManager()
{
    thread_stats_ = new ThreadStatistic*[g_txn_thread_num];
    for (uint64_t  i = 0; i < g_txn_thread_num; i++)
        thread_stats_[i] = new ThreadStatistic();
   
}

StatisticManager::~StatisticManager()
{
    for (uint64_t  i = 0; i < g_txn_thread_num; i++)
        delete thread_stats_[i];
    delete[] thread_stats_;
}


void StatisticManager::Reset()
{
    for (uint64_t thread_id = 0; thread_id < g_txn_thread_num; thread_id++)
        thread_stats_[thread_id]->ReSet();
}

void StatisticManager::ThreadStart(ThreadID thread_id)
{
    ThreadStatistic* thread_stat = thread_stats_[thread_id];
    GET_CLOCK_TIME(thread_stat->thread_start_time_);
}

void StatisticManager::ThreadEnd(ThreadID thread_id)
{
    ThreadStatistic* thread_stat = thread_stats_[thread_id];
    GET_CLOCK_TIME(thread_stat->thread_end_time_);
}

void StatisticManager::TxnCommit(ThreadID thread_id, uint64_t count)
{
    ThreadStatistic* thread_stat = thread_stats_[thread_id];
    thread_stat->txn_amount_ += count;
    thread_stat->txn_commit_ += count;
}


void StatisticManager::TxnAbort(ThreadID thread_id, uint64_t count)
{
    ThreadStatistic * thread_stat = thread_stats_[thread_id];
    thread_stat->txn_amount_ += count;
    thread_stat->txn_abort_ += count;
}

void StatisticManager::FetchLSNFail()
{
    ATOM_ADD(atom_fetch_lsn_fail_, 1);
}

void StatisticManager::PrintStatResult()
{
    uint64_t txn_amount = 0;
    uint64_t txn_commit = 0;
    uint64_t txn_abort  = 0;

    double test_time = (end_time_ - start_time_) / 1000000000.0;

    for (uint64_t thread_id = 0; thread_id < g_txn_thread_num; thread_id++)
    {
        ThreadStatistic * thread_stat = thread_stats_[thread_id];
        printf("thread %d throughput: %lf \n", thread_id, thread_stat->txn_commit_ / test_time);
        
        txn_amount += thread_stat->txn_amount_;
        txn_commit += thread_stat->txn_commit_;
        txn_abort  += thread_stat->txn_abort_;
    }
        
    double txn_throughput = txn_commit / test_time;

    printf("\n\nstatistic information: \n");    

    printf("the number of txns executed: %ld \n", txn_amount);
    printf("the number of txns committed: %ld \n", txn_commit);
    printf("the amount of txns aborted: %ld \n", txn_abort);
    printf("the ratio of txns committed: %lf \n", (double)txn_commit / txn_amount);
    printf("the ratio of txns aborted: %lf \n", (double)txn_abort / txn_amount);

    printf("the time of test: %lf secs \n", test_time);
    printf("the tps of transactional throughput: %lf \n", txn_throughput);

    printf("the time of atom fetch lsn fails: %ld \n", atom_fetch_lsn_fail_);
    printf("count1: %ld, count2: %ld\n", count1, count2);


    /***** for zhogndianyanfa *****/
    // printf("the number of new-order txns committed: %ld \n", txn_commit);
    // printf("the time of test: %lf mins \n", test_time);
    // printf("the tpmc of transactional throughput: %lf \n", txn_throughput);
    // printf("the number of new-order txns committed: %ld \n", 12162857);
    // printf("the time of test: %lf mins \n", 10.352939);
    // printf("the tpmc of transactional throughput: %lf \n", 1174821.642915);
}
