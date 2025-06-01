#ifndef RECLAIM_THREAD_STATISTIC_H_
#define RECLAIM_THREAD_STATISTIC_H_

#include "config.h"


class ReclaimThreadStatistic
{
public:
    ReclaimThreadStatistic();


    /******* Time Breakdown Analysis *******/

    ClockTime  reclaim_write_txn_time_;
    ClockTime  reclaim_idle_time_;



    /******* MICRO Statistic Analysis *******/

    uint64_t   reclaim_write_txn_sample_count_;
    uint64_t   vl_invisible_version_count_;
    uint64_t   vl_material_version_count_;

};




#endif