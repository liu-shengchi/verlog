#include "snapshot.h"



Snapshot::Snapshot()
{
    min_active_ts_ = 0;
    max_active_ts_ = 0;
    for (size_t i = 0; i < g_client_total_num; i++)
    {
        active_ts_[1]  = 0;
    }
}


Snapshot::~Snapshot()
{

}


bool Snapshot::IsVisible(Timestamp min_ts, Timestamp max_ts)
{
    bool visible = false;

    // if (max_active_ts_ < min_ts)
    // {
    //     visible = true;
    // }
    // else if (min_active_ts_ >= max_ts)
    // {
    //     visible = false;
    // }
    // else if (max_active_ts_ >= min_ts && min_active_ts_ < max_ts)
    // {
    //     for (uint64_t i = 0; i < g_client_total_num; i++)
    //     {
    //         if (active_ts_[i] < min_ts)
    //         {
    //             continue;
    //         }
    //         else if (active_ts_[i] < max_ts)
    //         {
    //             visible = true;
    //             break;
    //         }
    //         else if (active_ts_[i] >= max_ts)
    //         {
    //             visible = false;
    //             break;
    //         }
    //         else
    //         {
    //             printf("Snapshot::IsVisible: error! undefined snapshot && visible range!\n");
    //             exit(0);
    //         }
    //     }
    // }
    // else
    // {
    //     printf("Snapshot::IsVisible: error! undefined snapshot && visible range!\n");
    //     exit(0);
    // }

    if (min_active_ts_ < max_ts)
    {
        visible = true;
    }
    

    return visible;
}