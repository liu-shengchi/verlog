#ifndef   TRANSACTION_SNAPSHOT_H_
#define   TRANSACTION_SNAPSHOT_H_

#include "config.h"



class Snapshot
{
public:
    Snapshot();
    ~Snapshot();

    Timestamp min_active_ts_;
    Timestamp max_active_ts_;

    Timestamp active_ts_[g_client_total_num];


    bool IsVisible(Timestamp min_ts, Timestamp max_ts);
};





#endif