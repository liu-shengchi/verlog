#ifndef   SNAPSHOT_MANAGER_H_
#define   SNAPSHOT_MANAGER_H_

#include "config.h"

#include "db_rw_lock.h"


class Snapshot;

class SnapshotManager
{
private:
    
public:
    SnapshotManager();
    ~SnapshotManager();

    DBrwLock rw_lock_;

    alignas(CACHE_LINE_SIZE) volatile Timestamp rts_[g_client_total_num];

    void MakeSnapshot(Snapshot* snapshot);

    void BeforeFetchCTS(ClientID client_id);

    void AfterFetchCTS(ClientID client_id, Timestamp cts);

    void FinishActiveTxn(ClientID client_id);

};



#endif