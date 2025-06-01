#include "logger_thread.h"


#include "global.h"

#include "log_manager.h"

#include "log_buffer.h"

#include "tpcc_workload.h"

#include <pthread.h>
#include <cstddef>
#include <stdio.h>
#include <errno.h>
#include <thread>
#include <chrono>
#include "unistd.h"


LoggerThread::LoggerThread(ThreadID thread_id, ProcID process_id)
{
    thread_id_   = thread_id;
    thread_type_ = LOGGER_THREAD_T;

    processor_id_ = process_id;

    log_buf_id_ = g_log_manager->GetLogBufID(thread_id_, thread_type_);
    log_buffer_ = g_log_manager->GetLogBuffer(log_buf_id_);
}

LoggerThread::~LoggerThread()
{
}


void LoggerThread::Run()
{
    
#if THREAD_BIND_CORE == true
        SetAffinity();
#endif

    LogLSN   lsn            = 0;
    LogLSN   persistent_lsn = 0;
    LogLSN   replicated_lsn = 0;

    uint64_t flush_count    = 0;

    while (true)
    {
        lsn            = log_buffer_->lsn_;
        persistent_lsn = log_buffer_->persistented_lsn_;
        replicated_lsn = log_buffer_->replicated_lsn_;

#if PERSIST_LOG
        LogLSN   flush_lsn = UINT64_MAX;

        // for (uint64_t i = 0; i < g_txn_thread_num; i++)
        // {
        //     if (log_buffer_->filled_lsn_[i] == persistent_lsn &&
        //         log_buffer_->filling_[i]    == true)
        //     {
        //         i--;
        //         continue;
        //     }

        //     if (log_buffer_->filled_lsn_[i] > persistent_lsn &&
        //         log_buffer_->filled_lsn_[i] < flush_lsn)
        //     {
        //         flush_lsn = log_buffer_->filled_lsn_[i];
        //     }
        // }
        
        // log_buffer_->log_buf_spinlock_.GetSpinLock();
        // flush_lsn = log_buffer_->lsn_;
        // log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

        flush_lsn = log_buffer_->unpersistented_lsn_;

        COMPILER_BARRIER

        if (flush_lsn > persistent_lsn)   //存在需要刷盘的日志
        {
        #if MEM_LOG
            persistent_lsn = log_buffer_->persistented_lsn_ = flush_lsn;
        #else
            persistent_lsn = log_buffer_->FlushLog(flush_lsn);
        #endif
        }
#else
        log_buffer_->persistented_lsn_ = log_buffer_->lsn_;
#endif

        flush_count++;
        if (flush_count % 100000 == 0)
        {
            printf("log_buffer_id: %ld, lsn: %ld, persistent_lsn: %ld, replicated_lsn: %ld, g_commit_ts: %ld\n", 
                    log_buffer_->log_buffer_id_, lsn, persistent_lsn, replicated_lsn, g_commit_ts);
        }


#if BATCH_PERSIST
    /* 
        * 由于日志线程需要访问日志缓冲区、事务线程相关的元数据，
        * 不可避免地会在日志线程所在CPU的本地缓存中缓存元数据的只读副本，
        * 事务线程更新这些元数据时，会带来缓存一致性开销。
        * 因此，如果频繁调用日志线程访问日志缓冲区，则会影响事务吞吐。
        * 
        * 根据实际测试，8个事务线程，1个日志缓冲区，1个日志线程，在memlog模式下（不刷盘）
        * 不调用usleep 相较 调用usleep(50)，吞吐下降约10-15%。
        * 
        * 注意，usleep(0)实际也会使线程短暂暂停，因为调用usleep会触发系统中断，使线程进入等待状态，
        * 等待操作系统为线程分配CPU时间片。这个过程极为耗时，实际测试中，usleep(0)大约耗时40us。
        * 因此，调用usleep(0)和usleep(50)，效果接近。
        * 
        * 此外，如果磁盘每秒钟写I/O次数有限，如果频繁调用刷盘操作，每次刷盘数据量较小，
        * 则无法有效利用磁盘写入带宽。因此可以选择调用usleep，让日志线程间隔一段时间，将一批日志刷盘，
        * 利用批处理思想，提升日志刷盘I/O。
        */
    usleep(g_log_flush_interval);
        
#endif

        if (g_system_state == SystemState::FINISH_STATE)
        {
            break;
        }
    }

    printf("log buffer id: %ld, total log size: %ld, persistent_lsn: %ld, replicated_lsn: %ld, flush count: %ld\n",
            log_buffer_->log_buffer_id_, log_buffer_->lsn_, log_buffer_->persistented_lsn_, log_buffer_->replicated_lsn_, flush_count);
}

