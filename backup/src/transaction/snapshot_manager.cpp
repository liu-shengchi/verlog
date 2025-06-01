#include "snapshot_manager.h"

#include "global.h"

#include "snapshot.h"



SnapshotManager::SnapshotManager()
{
    for (size_t i = 0; i < g_client_total_num; i++)
    {
        rts_[i]  = 1;
    }
}

SnapshotManager::~SnapshotManager()
{
}

void SnapshotManager::MakeSnapshot(Snapshot* snapshot)
{
    rw_lock_.GetWriteLock();

    for (size_t i = 0; i < g_client_total_num; i++)
    {
        snapshot->active_ts_[i] = rts_[i];
    }
    
    rw_lock_.ReleaseWriteLock();

    // //将快照递增排序
    // bool isSwap = false;
    // for (int i = 0; i < g_client_total_num; i++)
    // {
    //     for (int j = 0; j < g_client_total_num - i; j++)
    //     { // 每次冒泡比较的元素
    //         if (snapshot->active_ts_[j] > snapshot->active_ts_[j + 1])
    //         {
    //             Timestamp temp = snapshot->active_ts_[j];
    //             snapshot->active_ts_[j] = snapshot->active_ts_[j+1];
    //             snapshot->active_ts_[j+1] = temp;
    //             isSwap = true;
    //         }
    //     }
    //     // 优化：没有交换代表数组已经完全有序
    //     if (!isSwap) {
    //         return;
    //     }
    // }

    // snapshot->min_active_ts_ = snapshot->active_ts_[0];

    // for (int i = 0; i < g_client_total_num; i++)
    // {
    //     if (snapshot->active_ts_[i] != MAX_TIMESTAMP)
    //     {
    //         snapshot->max_active_ts_ = snapshot->active_ts_[i];
    //     }
    //     else
    //     {
    //         break;
    //     }
    // }
    

    Timestamp min_ats = MAX_TIMESTAMP;

    for (uint64_t i = 0; i < g_client_total_num; i++)
    {
        if (min_ats > snapshot->active_ts_[i])
        {
            min_ats = snapshot->active_ts_[i];
        }
    }

    snapshot->min_active_ts_ = min_ats;
}



void SnapshotManager::BeforeFetchCTS(ClientID client_id)
{
    rw_lock_.GetReadLock();
}


void SnapshotManager::AfterFetchCTS(ClientID client_id, Timestamp cts)
{
    rts_[client_id] = cts;
    rw_lock_.ReleaseReadLock();
}

void SnapshotManager::FinishActiveTxn(ClientID client_id)
{
    rts_[client_id] = MAX_TIMESTAMP;
}
