#ifndef CLIENT_STATISTIC_H_
#define CLIENT_STATISTIC_H_

#include "config.h"




class ClientStatistic
{
public:
    ClientStatistic();

    void ReSet();

    ClockTime client_start_time_;
    ClockTime client_end_time_;

    ClockTime fetch_rts_time_;
    ClockTime wait_scr_snapshot_time_;
    ClockTime exec_txn_time_;

    Timestamp visibility_gap_;

    uint64_t txn_amount_;
    uint64_t txn_commit_;
    uint64_t txn_abort_;
};





#endif