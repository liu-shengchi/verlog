#ifndef REPLAY_THREAD_STATISTIC_H_
#define REPLAY_THREAD_STATISTIC_H_

#include "config.h"


class ReplayThreadStatistic
{
public:
    ReplayThreadStatistic();


    /******* Time Breakdown Analysis *******/

    // 以回放一个写事务为单位进行抽样
    uint64_t   time_break_down_sample_count_;

    ClockTime  replay_write_txn_time_array_[g_time_breakdown_max_sample_count];
    ClockTime  replay_idle_time_array_[g_time_breakdown_max_sample_count];

    ClockTime  replay_write_txn_time_;
    ClockTime  replay_idle_time_;

    ClockTime  load_analysis_log_time_;
    ClockTime  replay_payload_time_;

    ClockTime  eala_access_kv_index_time_;

    ClockTime  oa_record_write_info_time_;
    ClockTime  fa_create_version_time_;
    ClockTime  vl_maintain_log_chain_time_;




    /******* MICRO Statistic Analysis *******/

    uint64_t   micro_statc_sample_count_;

    uint64_t   replay_write_txn_sample_count_;

    uint64_t   ea_replay_access_kv_index_count_;
    uint64_t   ea_replay_create_version_count_;
    uint64_t   la_replay_record_unapplied_write_info_count_;
    uint64_t   la_replay_access_kv_index_count_;
    uint64_t   vl_replay_access_kv_index_count_;
    uint64_t   vl_fast_replay_path_count_;
};








#endif