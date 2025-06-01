#include "execute_thread_statistic.h"



ExecuteThreadStatistic::ExecuteThreadStatistic()
{
    time_break_down_sample_count_     = 0;
    execute_read_txn_ = 0;

    execute_read_request_ = 0;
    execute_txn_payload_  = 0;

    oa_fetch_unapplied_write_info_ = 0;
    oa_load_load_from_log_store_   = 0;
    oa_create_version_             = 0;



    micro_statc_sample_count_         = 0;

    la_execute_fetch_unapplied_write_info_count_ = 0;
    la_execute_load_log_count_        = 0;
    la_execute_access_disk_count_     = 0;
    la_execute_create_version_count_  = 0;
}
