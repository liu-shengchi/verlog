#ifndef THREAD_STATISTIC_H_
#define THREAD_STATISTIC_H_

#include "config.h"


class ThreadStatistic
{
public:
    ThreadStatistic();

    void ReSet();

    ClockTime thread_start_time_;
    ClockTime thread_end_time_;

    uint64_t txn_amount_;
    uint64_t txn_commit_;
    uint64_t txn_abort_;

    uint64_t atom_fetch_lsn_fail_;

};





#endif