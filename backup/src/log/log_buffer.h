#ifndef LOG_BUFFER_H_
#define LOG_BUFFER_H_

#include "config.h"

#include "db_spin_lock.h"


class LogReplayContext;
class TxnContext;
class Snapshot;

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


    char temp_buffer_[1024*1024] = {'\0'};

public:
    LogBuffer(uint64_t log_fd, LogBufID log_buf_id);
    ~LogBuffer();

    /* 
     * 将日志刷新到持久设备
     * 对logbuffer中的变量访问修改不保证原子性
     * 如果存在多线程并发调用FlushLog，需要在外部保证原子性
     * 
     * @param:  flush_lsn 表示将区间 [persistent_lsn_, flush_lsn) 内的日志全部刷入磁盘
     * @return: 返回persistent_lsn_
     * 
     * 注意，当持久化日志时，不能直接选择 [persistent_lsn_, lsn_) 区间持久化日志。
     * 在当前的实现中，向日志缓冲区同步日志时，首先根据日志大小扩大lsn_ 预留空间，
     * 之后再将日志复制到日志缓冲区。因此，区间 [persistent_lsn_, lsn_) 内可能存在
     * 还未完成日志同步的事务线程。因此需要日志持久化线程计算合理的 flush_lsn
     */
    LogLSN FlushLog(LogLSN  flush_lsn);

    uint64_t ReadFromDurable(char* buf, uint64_t read_size, LogLSN log_lsn);
    

    /* 
     * 由日志回放线程调用，获取下一个回放事务的日志。
     * 对于日志缓冲区来说，以事务为单位，一次向回放线程分配一个事务，
     * 对于回放线程来说，是以事务为粒度回放还是元组粒度回放，由回放策略决定。
     * 
     * @corner case: 
     *     事务日志跨环形日志缓冲区边界 | --> | --------- | --> |。这种情况下，
     * 会申请临时事务日志内存区域，拼接成完整的事务日志返回给回放线程，回放线程回放结束后
     * 需要手动释放txn_log。如果事务日志没有跨越缓冲区边界，则返回的txn_log指向缓冲区内
     * 事务日志的起始地址，回放线程不能释放。上述情况由buf_loop标识。
     * 
     * @param txn_log:  事务日志的起始地址
              buf_loop: 事务日志是否跨环形日志缓冲区的边界
     * @return 返回本次回放事务的起始LSN
     */

    bool GetReplayTxnLog(LogReplayContext* log_replay_context);
    


    uint64_t GetReclaimTxnLog(Snapshot* snapshot);

    /* 
     * LSN单调递增
     * 通过将lsn与buffer_size_求余，日志获取在日志缓冲区的位置
     * lsn_表示最小空闲位置的lsn，而非最大已使用位置，[lsn_, ∞) 表示日志空闲空间
     * persistent_lsn_表示还未持久化的最小lsn， [persistent_lsn_, ∞) 表示还未持久化的日志
     * 
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN persistented_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN free_lsn_;

    /* 
     * 日志回放相关数据域
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN    next_replay_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN    replayed_lsn_;
    alignas(CACHE_LINE_SIZE) volatile Timestamp replayed_commit_ts_;

    alignas(CACHE_LINE_SIZE) volatile uint64_t  replayer_num_;
    alignas(CACHE_LINE_SIZE) volatile bool      replaying_[g_replay_thread_num];
    alignas(CACHE_LINE_SIZE) volatile Timestamp replaying_commit_ts_[g_replay_thread_num];
    
    alignas(CACHE_LINE_SIZE) volatile LogLSN    replayed_lsn_per_replayer_[g_replay_thread_num];
    alignas(CACHE_LINE_SIZE) volatile Timestamp replayed_commit_ts_per_replayer_[g_replay_thread_num];
    

    /* 
     * 日志回收相关数据域
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN    protected_reclaim_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN    next_reclaim_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN    next_reclaimed_lsn_;
    alignas(CACHE_LINE_SIZE) volatile LogLSN    reclaimed_lsn_;
    alignas(CACHE_LINE_SIZE) volatile Timestamp reclaimed_ts_;


    alignas(CACHE_LINE_SIZE) volatile bool   modifying_[g_replay_thread_num];
    alignas(CACHE_LINE_SIZE) volatile LogLSN modifying_lsn_[g_replay_thread_num];


    alignas(CACHE_LINE_SIZE) volatile uint64_t  reclaimer_num_;
    alignas(CACHE_LINE_SIZE) volatile bool      reclaiming_[g_reclaim_thread_num];
    alignas(CACHE_LINE_SIZE) volatile LogLSN    reclaimed_lsn_per_reclaimer_[g_reclaim_thread_num];
    alignas(CACHE_LINE_SIZE) volatile Timestamp reclaimed_ts_per_reclaimer_[g_reclaim_thread_num];


    TxnContext* txn_contexts_[g_client_total_num] = {nullptr};

// #endif


    //单次I/O最小大小
    const uint32_t io_unit_size_;
    
    //日志文件描述符
    const uint64_t log_fd_;
    
    //日志缓冲区ID
    const LogBufID log_buf_id_;

    /* 
     * 环形日志缓冲区
     */
    const uint64_t buffer_size_;
    char*          buffer_;
    MemPtr         buffer_mem_ptr_;
};


#endif