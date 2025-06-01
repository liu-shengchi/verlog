#include "reclaim_thread_statistic.h"


ReclaimThreadStatistic::ReclaimThreadStatistic()
{

    reclaim_write_txn_time_ = 0;
    reclaim_idle_time_      = 0;


    reclaim_write_txn_sample_count_  = 0;

    vl_invisible_version_count_      = 0;
    vl_material_version_count_       = 0;
}
