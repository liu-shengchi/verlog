#ifndef EXECUTOR_LOAD_THREAD_H_
#define EXECUTOR_LOAD_THREAD_H_

#include "config.h"

#include "db_thread.h"

class LogBuffer;



class LoadThread : public DBThread
{
private:


#if  WORKLOAD_TYPE == TPCC_W

    uint64_t min_w_id_;
    uint64_t max_w_id_;

    void LoadItem();
    void LoadWarehouse(uint64_t  w_id);
    void LoadDistrict(uint64_t  w_id);
    void LoadStock(uint64_t  w_id);
    void LoadCustomer(uint64_t  w_id, uint64_t d_id);
    void LoadOrder(uint64_t  w_id, uint64_t d_id);
    void LoadHistory(uint64_t  w_id, uint64_t d_id, uint64_t c_id);
    

#elif  WORKLOAD_TYPE == YCSB_W

    uint64_t min_shard_;
    uint64_t max_shard_;

    void LoadYCSBTable();

#elif  WORKLOAD_TYPE == SMALLBANK_W

    // 当前load线程负责load数据的分区区间
    uint64_t min_shard_;
    uint64_t max_shard_;

    void LoadAccountTable(ShardID shard_id);
    void LoadSavingTable(ShardID shard_id);
    void LoadCheckingTable(ShardID shard_id);

#endif


    /* 初始化的元组，需要以事务日志的形式写入日志中
     * 每个元组构成一个事务日志，每个事务日志仅包含一个元组。
     * 事务日志的提交时间戳为0。
     */
#if  LOAD_DATA_LOG
    LogBuffer* log_buffer_;
    char*      log_entry_;
#endif


public:
    LoadThread(ThreadID thread_id, ProcID processor_id, uint64_t min_w_id, uint64_t max_w_id);
    ~LoadThread();

    void Run();
    
};



#endif