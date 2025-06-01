#ifndef EXECUTE_THREAD_STATISTIC_H_
#define EXECUTE_THREAD_STATISTIC_H_

#include "config.h"


class ExecuteThreadStatistic
{
public:
    ExecuteThreadStatistic();


    /******* Time Breakdown Analysis *******/

    ClockTime time_break_down_sample_count_;
    ClockTime execute_read_txn_;

    ClockTime execute_read_request_;
    ClockTime execute_txn_payload_;

    ClockTime oa_fetch_unapplied_write_info_;
    ClockTime oa_load_load_from_log_store_;
    ClockTime oa_create_version_;



    /******* MICRO Statistic Analysis *******/

    uint64_t   micro_statc_sample_count_;

    uint64_t  la_execute_fetch_unapplied_write_info_count_;
    uint64_t  la_execute_load_log_count_;
    uint64_t  la_execute_access_disk_count_;
    uint64_t  la_execute_create_version_count_;
    
};


#endif
