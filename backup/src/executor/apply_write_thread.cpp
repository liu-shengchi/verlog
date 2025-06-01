#include "apply_write_thread.h"

#include "txn_context.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "tuple.h"

#include "schema.h"

#include "apply_writes_on_demand_am.h"


#include "string.h"



#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

ApplyWriteThread::ApplyWriteThread(ThreadID thread_id, ThreadID apply_write_thread_id, ProcID processor_id)
{
    thread_type_  = DBThreadType::APPLY_WRITE_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = processor_id;

    apply_write_thread_id_   = apply_write_thread_id;
}


ApplyWriteThread::~ApplyWriteThread()
{

}


void ApplyWriteThread::Run()
{
#if THREAD_BIND_CORE
    SetAffinity();
#endif

    while (true)
    {
        ApplyWriteReq apply_write_req;

        // queue_lock_.GetSpinLock();

        // if (apply_write_queue_.empty())
        // {
        //     queue_lock_.ReleaseSpinLock();
        //     continue;
        // }
        // else
        // {
        //     apply_write_req = apply_write_queue_.front();
        //     apply_write_queue_.pop();
            
        //     queue_lock_.ReleaseSpinLock();
        // }

        array_lock_.GetLatch();
        if (head_slot_ == tail_slot_ && !is_full_)
        {
            //空，没有正在等待应用的写入
            array_lock_.ReleaseLatch();

            if (g_system_state != SystemState::FINISH_STATE)
            {
                continue;
            }
            else
            {
                break;
            }
        }
        else
        {
            apply_write_req = apply_write_array_[head_slot_];

            //更新队列状态
            head_slot_ = (head_slot_ + 1) % apply_write_array_length;
            is_full_   = false;

            array_lock_.ReleaseLatch();
        }
        
        
        // if (apply_write_req == nullptr)
        // {
        //     printf("error! apply_write_req cann't be equal to nullptr! ApplyWriteThread::Run \n");
        //     exit(0);
        // }
        

        WriteInfo* write_info = apply_write_req.write_info_;

        LogBufID    log_buf_id = (write_info->log_ptr_ & LOGBUF_MASK) >> LOGBUF_OFFSET;
        LogLSN      log_lsn    = (write_info->log_ptr_ & LOGLSN_MASK) >> LOGLSN_OFFSET;
        LogSize     log_size   = write_info->log_size_;

        LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);

        char log_entry[MAX_TUPLE_LOG_SIZE] = {'0'};
        bool in_log_buf = true;
        
        //从日志存储中获取log entry
        if (log_buf->free_lsn_ < log_lsn)
        {
            //日志处于日志缓冲区中
            COMPILER_BARRIER
            memcpy(log_entry, log_buf->buffer_ + (log_lsn % log_buf->buffer_size_), log_size);
            COMPILER_BARRIER

            //验证，元组日志是否还在日志缓冲区
            if (!(log_buf->free_lsn_ < log_lsn))
            {
                in_log_buf = false;
            }
        }
        else
        {
            in_log_buf = false;
        }

        if (!in_log_buf)
        {
            //日志不在日志缓冲区，向存储设备获取已持久化的日志。
            log_buf->ReadFromDurable(log_entry, log_size, log_lsn);

        #if MICRO_STATISTICAL_DATA_ANALYTIC
            if (apply_write_thread_id_ == 0)
            {
                count13++;
            }
        #endif            
        }


        //创建新版本
        Timestamp commit_ts   = write_info->commit_ts_;
        uint64_t  tuple_size  = g_schema->GetTupleSize(apply_write_req.table_id_);
        
        Tuple*    new_version = new Tuple(tuple_size, commit_ts);
        new_version->CopyTupleData((TupleData)((char*)log_entry) + 40, tuple_size);

        //将新版本插入版本链
        apply_write_req.kv_ao_->InsertNewVersion(new_version);

    #if MICRO_STATISTICAL_DATA_ANALYTIC
        if (apply_write_thread_id_ == 0)
        {
            count12++;
        }
    #endif

        COMPILER_BARRIER

        //更新write_info状态
        write_info->lock_.GetLatch();
        write_info->is_applied_  = true;   // 写入已被应用
        write_info->is_applying_ = false;  // 已结束应用写入
        write_info->lock_.ReleaseLatch();

        //更新TxnContext状态，应用写入已完成
        // apply_write_req->txn_context_->FinishApplyWrite();

        // delete apply_write_req;
    }
}

// void ApplyWriteThread::AppendApplyWriteReq(LogLogicalPtr log_ptr, 
//                                            uint32_t log_size,
//                                            Timestamp commit_ts,
//                                            TableID table_id,
//                                            ApplyWritesOnDemandAO* access_obj,
//                                            TxnContext* txn_context)
// {
//     queue_lock_.GetSpinLock();
//     apply_write_queue_.emplace(log_ptr, log_size, commit_ts, table_id, access_obj, txn_context);
//     queue_lock_.ReleaseSpinLock();
// }

void ApplyWriteThread::AppendApplyWriteReq(WriteInfo*             write_info,
                                           ApplyWritesOnDemandAO* kv_ao,
                                           TableID                table_id,
                                           TxnContext*            txn_context)
{
    // queue_lock_.GetSpinLock();

    // ApplyWriteReq* apply_write_req = new ApplyWriteReq();

    // apply_write_req->write_info_  = write_info;
    // apply_write_req->kv_ao_       = kv_ao;
    // apply_write_req->table_id_    = table_id;
    // apply_write_req->txn_context_ = txn_context;

    // if (apply_write_req == nullptr)
    // {
    //     printf("error! apply_write_req cann't be equal to nullptr! ApplyWriteThread::AppendApplyWriteReq");
    //     exit(0);
    // }
    

    // apply_write_queue_.push(apply_write_req);

    // queue_lock_.ReleaseSpinLock();



    // ApplyWriteReq* apply_write_req = new ApplyWriteReq();

    // apply_write_req->write_info_  = write_info;
    // apply_write_req->kv_ao_       = kv_ao;
    // apply_write_req->table_id_    = table_id;
    // apply_write_req->txn_context_ = txn_context;

    if (write_info == nullptr)
    {
        printf("error! write_info cann't be equal to nullptr! ApplyWriteThread::AppendApplyWriteReq");
        exit(0);
    }
    else if (kv_ao == nullptr)
    {
        printf("error! kv_ao cann't be equal to nullptr! ApplyWriteThread::AppendApplyWriteReq");
        exit(0);
    }
    else if (txn_context == nullptr)
    {
        printf("error! txn_context cann't be equal to nullptr! ApplyWriteThread::AppendApplyWriteReq");
        exit(0);
    }
    
    

    /* 
     * 加锁，并保证应用写的数组未满
     */
    bool hold_lock = false;
    do
    {
        array_lock_.GetLatch();
        hold_lock = true;

        if (is_full_)
        {
            array_lock_.ReleaseLatch();
            hold_lock = false;

            while (is_full_)
            {
                ;
            }
        }

        //到这里，is_full == false, 但是可能没有持锁
    } while (!hold_lock);


    //即持锁，也未满
    
    
    // apply_write_array_[tail_slot_] = apply_write_req;

    apply_write_array_[tail_slot_].write_info_  = write_info;
    apply_write_array_[tail_slot_].kv_ao_       = kv_ao;
    apply_write_array_[tail_slot_].table_id_    = table_id;
    apply_write_array_[tail_slot_].txn_context_ = txn_context;



    //占用tail_slot，更新array的状态
    tail_slot_ = (tail_slot_ + 1) % apply_write_array_length;
    if (tail_slot_ == head_slot_)
    {
        is_full_ = true;
    }
    
    array_lock_.ReleaseLatch();
}


#endif