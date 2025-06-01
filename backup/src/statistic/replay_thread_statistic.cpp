#include "replay_thread_statistic.h"


ReplayThreadStatistic::ReplayThreadStatistic()
{

    time_break_down_sample_count_ = 0;

    for (uint64_t i = 0; i < g_time_breakdown_max_sample_count; i++)
    {
        replay_write_txn_time_array_[i] = 0;
        replay_idle_time_array_[i]      = 0;
    }
    
    replay_write_txn_time_ = 0;
    replay_idle_time_      = 0;

    load_analysis_log_time_    = 0;
    replay_payload_time_       = 0;

    eala_access_kv_index_time_ = 0;

    oa_record_write_info_time_      = 0;
    fa_create_version_time_         = 0;
    vl_maintain_log_chain_time_     = 0;



    micro_statc_sample_count_        = 0;

    replay_write_txn_sample_count_   = 0;

    ea_replay_access_kv_index_count_ = 0;
    ea_replay_create_version_count_  = 0;
    la_replay_record_unapplied_write_info_count_ = 0;
    la_replay_access_kv_index_count_ = 0;
    vl_replay_access_kv_index_count_ = 0;
    vl_fast_replay_path_count_       = 0;

}



