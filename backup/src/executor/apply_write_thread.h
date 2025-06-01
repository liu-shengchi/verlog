#ifndef  APPLY_WRITE_THREAD_H_
#define  APPLY_WRITE_THREAD_H_

#include "config.h"
#include "global.h"

#include "db_latch.h"

#include "db_thread.h"


#include <queue>

using namespace std;



#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

class TxnContext;
class ApplyWritesOnDemandAO;
class WriteInfo;

class ApplyWriteThread : public DBThread
{
private:

    ThreadID                   apply_write_thread_id_;


    typedef struct ApplyWriteReq
    {
        // LogLogicalPtr  log_ptr   = 0X00;
        // uint32_t       log_size  = 0x00;

        // Timestamp      commit_ts = 0;

        WriteInfo*             write_info_  = nullptr;
        ApplyWritesOnDemandAO* kv_ao_       = nullptr;
        TableID                table_id_    = 0x00; 
        TxnContext*            txn_context_ = nullptr;

        // ApplyWriteReq(WriteInfo*             write_info,  
        //               ApplyWritesOnDemandAO* kv_ao, 
        //               TableID                table_id,
        //               TxnContext*            txn_context) 
        //             : write_info_(write_info), 
        //               kv_ao_(kv_ao),
        //               table_id_(table_id),
        //               txn_context_(txn_context) {}
    }ApplyWriteReq;


    // DBSpinLock                 queue_lock_;
    // std::queue<ApplyWriteReq*> apply_write_queue_;
    DBLatch           array_lock_;
    volatile uint64_t head_slot_ = 0;
    volatile uint64_t tail_slot_ = 0;
    volatile bool     is_full_   = false;
    ApplyWriteReq     apply_write_array_[apply_write_array_length];



public:
    ApplyWriteThread(ThreadID thread_id, ThreadID apply_write_thread_id, ProcID processor_id = 0);
    ~ApplyWriteThread();
    
    void Run();

    // void AppendApplyWriteReq(LogLogicalPtr log_ptr, 
    //                          uint32_t log_size,
    //                          Timestamp commit_ts,
    //                          TableID table_id,
    //                          ApplyWritesOnDemandAO* access_obj,
    //                          TxnContext* txn_context);


    void AppendApplyWriteReq(WriteInfo*             write_info,
                             ApplyWritesOnDemandAO* kv_ao,
                             TableID                table_id,
                             TxnContext*            txn_context);

};


#endif

#endif