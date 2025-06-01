#include "client_statistic.h"



ClientStatistic::ClientStatistic()
{
    client_start_time_ = 0;
    client_end_time_   = 0;
    
    fetch_rts_time_         = 0;
    wait_scr_snapshot_time_ = 0;
    exec_txn_time_          = 0;
    
    visibility_gap_         = 0;

    txn_amount_ = 0;
    txn_commit_ = 0;
    txn_abort_  = 0;
}


void ClientStatistic::ReSet()
{
    client_start_time_ = 0;
    client_end_time_   = 0;
    
    fetch_rts_time_         = 0;
    wait_scr_snapshot_time_ = 0;
    exec_txn_time_          = 0;
    
    txn_amount_ = 0;
    txn_commit_ = 0;
    txn_abort_  = 0;
}
