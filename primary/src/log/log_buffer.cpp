#include "log_buffer.h"

#include "statistic_manager.h"

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

LogBuffer::LogBuffer(uint64_t log_fd, LogBufID log_buffer_id)
            : log_fd_(log_fd), 
              log_buffer_id_(log_buffer_id),
              buffer_size_(g_log_buffer_size),
              io_unit_size_(IO_UNIT_SIZE)
{
    lsn_            = 0;

    unpersistented_lsn_ = 0;
    unreplicated_lsn_   = 0;

    persistented_lsn_ = 0;
    replicated_lsn_   = 0;

    for (uint64_t i = 0; i < g_txn_thread_num; i++)
    {
        filled_lsn_[i] = 0;
        filling_[i]    = false;
    }

    buffer_      = (char*)malloc(buffer_size_);

    memset(buffer_, 0, buffer_size_);
}

LogBuffer::~LogBuffer()
{
    free(buffer_);
}


LogLSN LogBuffer::AtomicFetchLSN(uint64_t log_size)
{
    LogLSN start_lsn = 0;
    LogLSN end_lsn = 0;

    /* 
     * 原子操作，获取存放当前日志的日志缓冲区空间
     * 保证新申请的日志空间不会覆盖掉还未flush的日志
     */
    while (true)
    {
        start_lsn = lsn_;
        end_lsn = start_lsn + log_size;

#if PRIMARY_BACKUP
        if (end_lsn - persistented_lsn_ >= buffer_size_ || end_lsn - replicated_lsn_ >= buffer_size_)
#else
        if (end_lsn - persistented_lsn_ >= buffer_size_)
#endif
        {
            // printf("log buffer overlap!\n");
            g_statistic_manager->FetchLSNFail();
            PAUSE
            continue;
        }

        /* 使用CAS原子操作修改lsn_
         * 原理：
         *   判断开始获取的lsn是否改变，如果未改变，说明没有并发事务并发获取lsn，
         *   则将lsn_改为end_lsn。如果改变，则说明存在原子获取LSN竞争，则重试
         */
        if (ATOM_CAS(lsn_, start_lsn, end_lsn))
            break;

        // printf("获取LSN出现竞争!\n");
        // g_statistic_manager->FetchLSNFail();
    }

    return start_lsn;
}


void LogBuffer::SynLogToBuffer(LogLSN start_lsn, char* log_entry, uint64_t log_size)
{
    if (log_size == 0)
    {
        return;
    }
    
    LogLSN end_lsn = start_lsn + log_size - 1;

    if (end_lsn / buffer_size_ > start_lsn / buffer_size_)
    {
        //日志空间跨越日志缓冲区
        // || r2-> | ----------- | r1-> ||
        uint64_t leave_size = end_lsn % buffer_size_ + 1; //区间 r2 的大小
        memcpy(buffer_ + start_lsn % buffer_size_, log_entry, log_size - leave_size);  //填充r1
        memcpy(buffer_, log_entry + (log_size - leave_size), leave_size); //填充r2
    }
    else
    {
        //日志空间在日志缓冲空间中连续存放
        //|| ------ | r1-> | ------- ||
        memcpy(buffer_ + start_lsn % buffer_size_, log_entry, log_size); //填充r1
    }

    COMPILER_BARRIER
}



LogLSN LogBuffer::FlushLog(LogLSN flush_lsn)
{
    uint64_t flush_size = flush_lsn - persistented_lsn_;

    if (flush_size == 0)
    {
        return persistented_lsn_;
    }

    if (flush_size > io_unit_size_)
    {
        //如果刷盘日志大小超过一个io_unit，则以io_unit_size为单位持久化日志
        flush_size = flush_size - flush_size % io_unit_size_;
    }

    //flush区间 [persistented_lsn_, persistented_lsn_ + flush_size) 的日志
    Flush(persistented_lsn_, flush_size);

    COMPILER_BARRIER

    persistented_lsn_ += flush_size;

    return persistented_lsn_;
}

void LogBuffer::Flush(LogLSN flush_start_lsn, uint64_t flush_size)
{
    uint64_t flush_bits = 0;

    LogLSN flush_end_lsn = flush_start_lsn + flush_size - 1;

    /** TODO: 存在错误，待修改！ **/
    if (flush_end_lsn / buffer_size_ > flush_start_lsn / buffer_size_)
    {
        uint64_t leave_size = flush_end_lsn % buffer_size_ + 1;
        flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size - leave_size);
        flush_bits += write(log_fd_, (void*)buffer_, leave_size);
    }
    else
    {
        flush_bits += write(log_fd_, (void*)(buffer_ + flush_start_lsn % buffer_size_), flush_size);
    }

    if (flush_bits != flush_size)
    {
        fprintf(stderr, "write error with errno=%d_%s, log_fd: %ld\n", errno, strerror(errno), log_fd_);
    }
}
