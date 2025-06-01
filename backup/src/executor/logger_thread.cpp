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


LoggerThread::LoggerThread(ThreadID thread_id, ThreadID logger_thread_id, ProcID process_id)
{
    thread_type_ = LOGGER_THREAD_T;
    thread_id_   = thread_id;
    processor_id_ = process_id;

    logger_thread_id_ = logger_thread_id;

    log_buf_id_ = g_log_manager->GetLogBufID(thread_id_, thread_type_);
    log_buffer_ = g_log_manager->GetLogBuffer(log_buf_id_);

    printf("logger: %ld, log_buf: %ld\n", thread_id, log_buf_id_);
}

LoggerThread::~LoggerThread()
{
}


void LoggerThread::SetAffinity()
{
    /*
     * 按照thread_id_，依次绑定CPU核心
     */
    //+24 表示使用第二个NUMA节点
    //+ g_txn_thread_nu 表示在事务线程后，分配CPU核心
    // uint64_t processor_id = thread_id_ + 24 + g_txn_thread_num;   

    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(processor_id_, &cpu_set);
    
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}


void LoggerThread::Run()
{
#if THREAD_BIND_CORE == true
        SetAffinity();
#endif

    LogLSN   lsn              = 0;
    LogLSN   persistented_lsn = 0;
    LogLSN   flush_lsn        = 0;
    uint64_t flush_count      = 0;

    while (g_system_state != SystemState::FINISH_STATE)
    {
        lsn              = log_buffer_->lsn_;
        persistented_lsn = log_buffer_->persistented_lsn_;



#if PERSIST_LOG

        LogLSN flush_lsn = log_buffer_->lsn_;


        COMPILER_BARRIER

        if (flush_lsn > persistented_lsn)   //存在需要刷盘的日志
        {
        #if MEM_LOG
            persistented_lsn = log_buffer_->persistented_lsn_ = flush_lsn;
        #else
            persistented_lsn = log_buffer_->FlushLog(flush_lsn);
        #endif
        }
#else
        log_buffer_->persistented_lsn_ = log_buffer_->lsn_;
#endif
        

        flush_count++;
        // if (flush_count % 10000000 == 0)
        // {
        //     printf("log_buffer_id: %ld, lsn: %ld, persistented_lsn: %ld, replayed_lsn: %ld, free_lsn: %ld, g_commit_ts: %ld, g_visible_ts: %ld, g_reclaimed_ts: %ld\n", 
        //                                             log_buffer_->log_buf_id_, 
        //                                             lsn, 
        //                                             persistented_lsn,
        //                                             log_buffer_->replayed_lsn_,
        //                                             log_buffer_->free_lsn_,
        //                                             g_commit_ts,
        //                                             g_visible_ts,
        //                                             g_reclaimed_ts);
        // }


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
    }

        printf("log_buffer_id: %ld, lsn: %ld, persistented_lsn: %ld, replayed_lsn: %ld, free_lsn: %ld, g_commit_ts: %ld, g_visible_ts: %ld, g_reclaimed_ts: %ld\n", 
                                                log_buffer_->log_buf_id_, 
                                                log_buffer_->lsn_, 
                                                log_buffer_->persistented_lsn_,
                                                log_buffer_->replayed_lsn_,
                                                log_buffer_->free_lsn_,
                                                g_commit_ts,
                                                g_visible_ts,
                                                g_reclaimed_ts);

//     printf("log buffer id: %ld, total log size: %ld, persistented lsn: %ld, replaying lsn: %ld\n",
//                                         log_buffer_->log_buf_id_, 
//                                         log_buffer_->lsn_, 
//                                         log_buffer_->persistented_lsn_,
//                                         log_buffer_->next_replay_lsn_);

}

