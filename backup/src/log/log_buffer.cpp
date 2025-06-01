#include "log_buffer.h"

#include "statistic_manager.h"

#include "snapshot_manager.h"

#include "log_strategy.h"
#include "log_replay_context.h"

#include "txn_context.h"

#include "unistd.h"
#include "string.h"
#include <malloc.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


using namespace std;



/**************************************/
/************ LogBuffer ***************/
/**************************************/

LogBuffer::LogBuffer(uint64_t log_fd, LogBufID log_buf_id)
            : log_fd_(log_fd), 
              log_buf_id_(log_buf_id),
              buffer_size_(g_log_buffer_size),
              io_unit_size_(IO_UNIT_SIZE)
{
    lsn_                = 0;
    persistented_lsn_   = 0;
    free_lsn_           = 0;

    /* 
     * 日志回放相关数据域
     */
    next_replay_lsn_ = 0;
    replayed_lsn_       = 0;
    replayed_commit_ts_ = 0;
    
    replayer_num_ = 0;
    int alloced_thread   = 0;
    int unalloced_thread = g_replay_thread_num;
    while (alloced_thread < g_replay_thread_num)
    {
        int max_comm_div = unalloced_thread;
        for (; max_comm_div >= 1; max_comm_div--)
        {
            if (g_log_buffer_num % max_comm_div == 0)
                break;
        }
        
        alloced_thread   += max_comm_div;
        unalloced_thread -= max_comm_div;
        
        replayer_num_++;
        continue;
    }

    for (size_t i = 0; i < g_replay_thread_num; i++)
    {
        replaying_[i]                       = false;
        replaying_commit_ts_[i]             = 0;

        replayed_lsn_per_replayer_[i]       = 0;
        replayed_commit_ts_per_replayer_[i] = 0;
    }


    /* 
     * 日志回收相关数据域
     */
    protected_reclaim_lsn_ = 0;
    next_reclaim_lsn_      = 0;
    next_reclaimed_lsn_    = 0;
    reclaimed_lsn_         = 0;
    reclaimed_ts_          = 0;

    for (size_t i = 0; i < g_replay_thread_num; i++)
    {
        modifying_[i] = false;
        modifying_lsn_[i] = 0;
    }



    reclaimer_num_   = 0;
    alloced_thread   = 0;
    unalloced_thread = g_reclaim_thread_num;
    while (alloced_thread < g_reclaim_thread_num)
    {
        int max_comm_div = unalloced_thread;
        for (; max_comm_div >= 1; max_comm_div--)
        {
            if (g_log_buffer_num % max_comm_div == 0)
                break;
        }
        
        alloced_thread   += max_comm_div;
        unalloced_thread -= max_comm_div;
        
        reclaimer_num_++;
        continue;
    }

    for (size_t i = 0; i < g_reclaim_thread_num; i++)
    {
        reclaiming_[i] = false;
        reclaimed_lsn_per_reclaimer_[i] = 0;
        reclaimed_ts_per_reclaimer_[i]  = 0;
    }


    //日志缓冲区
    buffer_         = (char*)malloc(buffer_size_ + MAX_TXN_LOG_SIZE);
    buffer_mem_ptr_ = (MemPtr)buffer_;
    memset(buffer_, 0, buffer_size_ + MAX_TXN_LOG_SIZE);
}

LogBuffer::~LogBuffer()
{
    free(buffer_);
}


LogLSN LogBuffer::FlushLog(LogLSN  flush_lsn)
{
    uint64_t flush_size = flush_lsn - persistented_lsn_;

    // if (flush_size == 0)
    // {
    //     return persistented_lsn_;
    // }

    if (flush_size >= io_unit_size_)
    {
        //如果刷盘日志大小超过一个io_unit，则以io_unit_size为单位持久化日志
        flush_size = flush_size - flush_size % io_unit_size_;

        if (flush_size > 1024*1024)
        {
            //不能超过 temp_buffer
            flush_size = 1024*1024;
        }
        

        //flush区间 [persistented_lsn_, persistented_lsn_ + flush_size) 的日志
        Flush(persistented_lsn_, flush_size);

        COMPILER_BARRIER

        persistented_lsn_ += flush_size;
    }
    else
    {
        //如果刷盘日志大小不足一个io_unit，则不刷新
    }

    return persistented_lsn_;
}

void LogBuffer::Flush(LogLSN flush_start_lsn, uint64_t flush_size)
{
    uint64_t flush_bits = 0;

    LogLSN flush_end_lsn = flush_start_lsn + flush_size - 1;

    // if (flush_end_lsn / buffer_size_ > flush_start_lsn / buffer_size_)
    // {
    //     uint64_t leave_size = flush_end_lsn % buffer_size_ + 1;
    //     flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size - leave_size);
    //     flush_bits += write(log_fd_, (void*)buffer_, leave_size);
    // }
    // else
    // {
    //     flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size);
    // }


    if (flush_end_lsn / buffer_size_ > flush_start_lsn / buffer_size_)
    {
        uint64_t leave_size = flush_end_lsn % buffer_size_ + 1;
        // flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size - leave_size);
        // flush_bits += write(log_fd_, (void*)buffer_, leave_size);

        memcpy(temp_buffer_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size - leave_size);
        memcpy((void*)(temp_buffer_ + flush_size - leave_size), (void*)buffer_, leave_size);

    }
    else
    {
        // flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size);
        memcpy(temp_buffer_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size);
    }

    flush_bits += write(log_fd_, (void*)temp_buffer_, flush_size);


    
    if (flush_bits != flush_size)
    {
        fprintf(stderr, "write error with errno=%d_%s, log_fd: %ld, flush_size %ld, flush_bits: %ld, \n",
                         errno, strerror(errno), log_fd_, flush_size, flush_bits);
        exit(0);
    }
}


uint64_t LogBuffer::ReadFromDurable(char* buf, uint64_t read_size, LogLSN log_lsn)
{
    size_t size = 0;
    while (size < read_size) {
        ssize_t n = pread(log_fd_, buf + size, read_size - size, log_lsn + size);
        // if (not n) 
        //     break;
        
        if (n < 0)
        {
            fprintf(stderr, "read error with errno=%d_%s, log_fd: %ld, read_size %ld, log_lsn: %ld, size: %ld\n",
                         errno, strerror(errno), log_fd_, read_size, log_lsn, size);
        }
        
        size += n;
    }
    return size;
}



bool LogBuffer::GetReplayTxnLog(LogReplayContext* log_replay_context)
{
    LogLSN   replay_start_lsn = 0;
    LogLSN   replay_end_lsn   = 0;
    uint64_t txn_log_size     = 0;

    while (true)
    {
        replay_start_lsn = next_replay_lsn_;
        
        //需要回放的日志还未复制到备节点
        if (replay_start_lsn + 16 >= lsn_)
        {
            return false;
        }

        /* 
         * 事务日志包含的范围是 [replay_start_lsn, replay_start_lsn + log_size)
         * log_size在事务日志的第 [replay_start_lsn, replay_start_lsn + 7] 字节内，共8字节
         */
        uint64_t log_size_start_lsn = replay_start_lsn;
        uint64_t log_size_end_lsn   = replay_start_lsn + 8 - 1;

        char* log_size_ptr = buffer_ + log_size_start_lsn % buffer_size_;

        /*
         * 获取事务日志的大小
         * 需要分类讨论：
         * 1. 日志的log_size属性域在log buffer中连续存放 || ----- | r1-> | ----- ||
         * 2. 日志的log_size属性域被log buffer截为两部分 || r2-> | ------ | r1-> ||
         */
        if (log_size_start_lsn / buffer_size_ == log_size_end_lsn / buffer_size_)
        {
            txn_log_size = *(uint64_t*)log_size_ptr;
        }
        else
        {
            char log_size_data[8];
            uint16_t leave_size = log_size_end_lsn % buffer_size_ + 1;
            
            memcpy(log_size_data, log_size_ptr, 8 - leave_size);
            memcpy(log_size_data + 8 - leave_size, buffer_, leave_size);
            
            txn_log_size = *(uint64_t*)log_size_data;

            printf("txn_log_size: %ld..........\n", txn_log_size);
        }
        
        replay_end_lsn = replay_start_lsn + txn_log_size;

        if (txn_log_size == 0 || txn_log_size >= 1000000)
        {
            int a = 0;
        }
        

        //事务日志还未完全复制到备节点日志缓冲区
        if (replay_end_lsn > lsn_)
        {
            // if (g_system_state == SystemState::TESTING_STATE)
            //     g_statistic_manager->GetReplayTxnLogFail();
            
            return false;
        }
        
        COMPILER_BARRIER
        
        //原子更新next_replay_lsn_
        if (ATOM_CAS(next_replay_lsn_, replay_start_lsn, replay_end_lsn))
            break;
    }

    LogTxnMeta* log_txn_meta = log_replay_context->log_txn_meta_;

    log_txn_meta->log_buf_id_ = log_buf_id_;
    log_txn_meta->start_lsn_  = replay_start_lsn;
    log_txn_meta->log_size_   = txn_log_size;
    
    log_txn_meta->log_start_ptr_ = ((MemPtr)buffer_) + (replay_start_lsn % g_log_buffer_size);
    
    //事务日志跨日志缓冲区边界
    if (replay_start_lsn / buffer_size_ != (replay_end_lsn - 1) / buffer_size_)
    {
        uint64_t leave_size = replay_end_lsn % buffer_size_;
        memcpy((char*)(buffer_ + g_log_buffer_size), (char*)buffer_, leave_size);
    }
    
    return true;
}


uint64_t LogBuffer::GetReclaimTxnLog(Snapshot* snapshot)
{
    LogLSN   reclaim_start_lsn = 0;
    LogLSN   reclaim_end_lsn   = 0;
    uint64_t log_size          = 0;

    while (true)
    {
        reclaim_start_lsn = next_reclaim_lsn_;
        log_size          = *(uint64_t*)(buffer_ + (reclaim_start_lsn % g_log_buffer_size));
        reclaim_end_lsn   = reclaim_start_lsn + log_size;

        if (log_size == 0 || log_size > 100000)
        {
            printf("回收线程解析出错误logsize!\n");
        }

        /* 
         * 受保护的日志空间不足，无法确定日志允许被回收
         * 需要扩展protected_reclaim_lsn
         */
        uint64_t temp = protected_reclaim_lsn_;
        if (reclaim_end_lsn > protected_reclaim_lsn_)
        {
            /* 原子修改，避免并发回收线程修改 protected_reclaim_lsn */
            ATOM_CAS(protected_reclaim_lsn_, temp, temp + g_reclaim_log_interval);

            /*
             * 保证[0, protected_reclaim_lsn_)区间内的日志，已经回放，
             */
            COMPILER_BARRIER
            while (protected_reclaim_lsn_ > replayed_lsn_)
                PAUSE

            COMPILER_BARRIER

            /* 
             * 保证[0, protected_reclaim_lsn_)区间内的日志不会被回放线程修改
             */
        #if  AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
            for (uint64_t i = 0; i < g_replay_thread_num; i++)
            {
                while (modifying_[i])
                {
                    if (modifying_lsn_[i] >= protected_reclaim_lsn_)
                        break;
                    ;
                }
            }
        #endif

            COMPILER_BARRIER
            
            /* 
             * 确保事务线程不会再访问区间[0, next_reclaimed_lsn_)的日志
             */
            if (g_system_state == LOADING_STATE)
            {
                ;
            }
            else
            {
                for (ClientID client_id = 0; client_id < g_client_total_num; client_id++)
                {
                    TxnContext* txn_context = txn_contexts_[client_id];
                    while (txn_context->accessing_logbuf_[log_buf_id_])
                    {
                        // if (txn_context->accessing_lsn_[log_buf_id_] >= next_reclaimed_lsn_ || g_system_state == FINISH_STATE)
                        if (txn_context->accessing_lsn_[log_buf_id_] >= next_reclaimed_lsn_ || g_system_state == FINISH_STATE)
                            break;
                    }
                }
            }

            COMPILER_BARRIER


            reclaimed_lsn_ = next_reclaimed_lsn_;

            /* 
             * 计算当前系统快照。
             */
            // g_snapshot_manager->MakeSnapshot(snapshot);
        }
        
        if (ATOM_CAS(next_reclaim_lsn_, reclaim_start_lsn, reclaim_end_lsn))
            break;
    }
    
    return reclaim_start_lsn;
}
