#ifndef LOG_BUFFER_H_
#define LOG_BUFFER_H_

#include "config.h"

#include "db_spin_lock.h"
#include "db_rw_lock.h"

/* 
 * 日志缓冲区
 * 日志缓冲区需要维护日志最新的LSN和已经完成持久化的LSN（persistent_lsn）
 * 
 * TODO：
 *     主备复制，需要记录已复制的LSN，logbuffer需要控制哪些日志区间允许进行复制。
 */

class LogBuffer
{
private:

    void Flush(LogLSN  flush_start_lsn, uint64_t flush_size);

public:
    LogBuffer(uint64_t log_fd, LogBufID log_buffer_id);
    ~LogBuffer();

    /* 
     * 原子获取该日志流的LSN，
     * @return: 返回日志的起始lsn，
     * 
     * 对于调用者，[rerturn_lsn, rerturn_lsn + logsize) 为存放日志的空间
     */
    LogLSN AtomicFetchLSN(uint64_t log_size);
    
    /* 
     * 将日志内容按照指定起始位置和大小，同步到日志缓冲区内。
     * 
     * 注意，调用该函数前，调用者须调用AtomicFetchLSN，获取LSN和存放日志的空间，
     * [start_lsn, start_lsn + log_size)即在日志缓冲区中预留的日志空间
     */
    void   SynLogToBuffer(LogLSN start_lsn, char* log_entry, uint64_t log_size);


    /* 
     * 将日志记录写入到日志缓冲区
     * 保证对LogBuffer中变量访问修改的原子性，允许多线程并发调用
     * @return: 返回日志的起始LSN；如果空间不足插入失败，返回UINT64_MAX
     */
    // uint64_t AppendLog(char* log_entry, uint32_t log_size);

    /* 
     * 将日志刷新到持久设备
     * 对logbuffer中的变量访问修改不保证原子性
     * 如果存在多线程并发调用FlushLog，需要在外部保证原子性
     * 
     * @param:  flush_lsn 表示将区间 [persistented_lsn_, flush_lsn) 内的日志全部刷入磁盘
     * @return: 返回persistented_lsn_
     * 
     * 注意，当持久化日志时，不能直接选择 [persistented_lsn_, lsn_) 区间持久化日志。
     * 在当前的实现中，向日志缓冲区同步日志时，首先根据日志大小扩大lsn_ 预留空间，
     * 之后再将日志复制到日志缓冲区。因此，区间 [persistented_lsn_, lsn_) 内可能存在
     * 还未完成日志同步的事务线程。因此需要日志持久化线程计算合理的 flush_lsn
     */
    LogLSN FlushLog(LogLSN  flush_lsn);



    alignas(CACHE_LINE_SIZE) DBSpinLock log_buf_spinlock_;

    alignas(CACHE_LINE_SIZE) DBrwLock   log_buf_rwlock_;

    /* 
     * LSN单调递增
     * 通过将lsn与buffer_size_求余，日志获取在日志缓冲区的位置
     * lsn_表示最小空闲位置的lsn，而非最大已使用位置，[lsn_, ∞) 表示日志空闲空间
     * persistented_lsn_表示还未持久化的最小lsn， [persistented_lsn_, ∞) 表示还未持久化的日志
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN lsn_;

    alignas(CACHE_LINE_SIZE) volatile LogLSN unpersistented_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN unreplicated_lsn_;
    
    alignas(CACHE_LINE_SIZE) volatile LogLSN persistented_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN replicated_lsn_;
    
    /* 
     * filled_lsn_记录每个事务线程最近一次向该日志缓冲区同步日志的最大LSN，
     * 表示 [0, filled_lsn_)区间内的日志，该事务线程已同步到日志缓冲区，可以持久化、复制。
     * 
     * 日志持久化线程每次持久化日志前，先确定可刷新的flush_lsn，初始化为MAX，
     * 遍历filled_lsn_，如果filled_lsn_[i] > persistented_lsn_，则
     * flush_lsn = min(flush_lsn, filled_lsn_[i])。flush_lsn表示区间
     * [0, flush_lsn)内的日志事务线程已经全部同步到日志缓冲区中，可以持久化。
     * 
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN filled_lsn_[g_txn_thread_num];
    alignas(CACHE_LINE_SIZE) volatile bool   filling_[g_txn_thread_num];


    //单次I/O最小大小
    const uint32_t io_unit_size_;
    
    //日志文件描述符
    const uint64_t log_fd_;
    
    //日志缓冲区ID
    const LogBufID log_buffer_id_;

    /* 
     * 环形日志缓冲区
     */
    const uint64_t buffer_size_;
    char*          buffer_;
    
};


#endif