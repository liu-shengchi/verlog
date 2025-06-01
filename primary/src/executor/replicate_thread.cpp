#include "replicate_thread.h"


#include "comm_manager.h"
#include "log_replica_comm.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "workload.h"


ReplicateThread::ReplicateThread(ThreadID thread_id, ProcID process_id)
{
    thread_id_    = thread_id;
    processor_id_ = process_id;
}

ReplicateThread::~ReplicateThread()
{
}


void ReplicateThread::Run()
{
    LogReplicaComm* log_replica_comm = nullptr;
    LogBuffer*      log_buf          = nullptr;

    LogLSN          unreplicated_lsn = 0;
    LogLSN          replicating_lsn  = 0;
    uint32_t        replica_size     = 0;   //一次复制日志大小不能超过4GB

    MemPtr          send_addr        = 0;
    MemPtr          recv_addr        = 0;

    uint64_t        local_log_buf_size  = g_log_buffer_size;
    uint64_t        remote_log_buf_size = g_backup_log_buf_size;


    while (g_system_state != SystemState::FINISH_STATE)
    {
        for (uint64_t i = 0; i < 10000; i++)
        {
            for (uint64_t i = 0; i < g_comm_manager->log_replica_comm_cnt_; i++)
            {
                //不由该复制线程负责
                if (i % g_replica_thread_num != (thread_id_ - replica_thread_id_offset))
                    continue;

                log_replica_comm = g_comm_manager->log_replica_comms_[i];

                /** 接收来自备节点的message，处理相关任务（同步reclaimed_lsn） **/        
                log_replica_comm->ProcessRecvWC();

                /** 处理复制日志wr处理结束后生成的wc **/
                log_replica_comm->ProcessSendWC();
                
                if (log_replica_comm->outstanding_wr_ == g_qp_max_depth)
                    continue;
                

                /** 复制日志 **/
                log_buf = log_replica_comm->primary_log_buf_;

                unreplicated_lsn = log_buf->unreplicated_lsn_;
                replicating_lsn  = log_replica_comm->replicating_lsn_;
                replica_size     = unreplicated_lsn - replicating_lsn;

                //未到单次日志复制大小的阈值
                if (replica_size <= g_log_replica_threshold)
                    continue;


            #if  SYN_RECLAIMED_LOG
                //备节点的日志缓冲区中没有足够的空闲空间接收复制日志
                if (replicating_lsn - log_replica_comm->reclaimed_lsn_ >= remote_log_buf_size)
                    continue;

                if (unreplicated_lsn - log_replica_comm->reclaimed_lsn_ > remote_log_buf_size)
                {
                    unreplicated_lsn = remote_log_buf_size + log_replica_comm->reclaimed_lsn_;
                    replica_size     = unreplicated_lsn - replicating_lsn;
                }
            #endif

                /*
                 * 事务线程首先增加lsn获取日志空间，之后将日志从本地同步到日志缓冲区。
                 * 首先确定[replicated_lsn, unreplicated_lsn)区间内日志已经填充完毕（没有空洞）。
                 */
                COMPILER_BARRIER

                // bool filled;
                // do
                // {
                //     filled = true;
                //     for (size_t i = 0; i < g_txn_thread_num; i++)
                //     {
                //         if (!log_buf->filling_[i])
                //             continue;
                        
                //         COMPILER_BARRIER

                //         if (log_buf->filled_lsn_[i] < unreplicated_lsn)
                //         {
                //             PAUSE
                //             filled = false;
                //             break;
                //         }
                //     }
                // } while (!filled);
                /*********************/
                // for (size_t i = 0; i < g_txn_thread_num; i++)
                // {
                //     if (!log_buf->filling_[i])
                //         continue;
                //     else
                //     {
                //         if (log_buf->filled_lsn_[i] >= unreplicated_lsn)
                //             continue;
                //         else
                //             while (log_buf->filling_[i])
                //                 PAUSE
                //     }
                // }

                COMPILER_BARRIER
                


                while (unreplicated_lsn != replicating_lsn)
                {
                    send_addr = replicating_lsn % local_log_buf_size + log_replica_comm->local_log_buf_mr_meta_.mr_addr;
                    recv_addr = replicating_lsn % remote_log_buf_size + log_replica_comm->remote_log_buf_mr_meta_.mr_addr;

                    //复制的日志区间是否跨本地日志缓冲区
                    if (replicating_lsn / local_log_buf_size == (unreplicated_lsn - 1) / local_log_buf_size)
                    {
                        if (replicating_lsn / remote_log_buf_size == (unreplicated_lsn - 1) / remote_log_buf_size)
                            //复制的日志既没有跨本地日志缓冲区，也没有跨远端日志缓冲区
                            replica_size = unreplicated_lsn - replicating_lsn;
                        else
                            //复制的日志没有跨本地日志缓冲区，但是跨远端日志缓冲区
                            replica_size = remote_log_buf_size - replicating_lsn % remote_log_buf_size;
                    }
                    else
                    {
                        //复制的日志跨本地日志缓冲区
                        replica_size = local_log_buf_size - replicating_lsn % local_log_buf_size;

                        if (replicating_lsn / remote_log_buf_size != (replicating_lsn + replica_size - 1) / remote_log_buf_size)
                            //复制的日志跨本地日志缓冲区，同时跨远端日志缓冲区
                            replica_size = remote_log_buf_size - replicating_lsn % remote_log_buf_size;
                    }

                    log_replica_comm->ReplicateLog(send_addr, recv_addr, replica_size);
                    COMPILER_BARRIER
                    replicating_lsn += replica_size;
                }

                COMPILER_BARRIER

                if (unreplicated_lsn != replicating_lsn)
                {
                    printf("不相等!\n");
                }

                //修改replicating_lsn，并未修改replicated_lsn。后续通过接收处理wc来修改replicated_lsn
                log_replica_comm->replicating_lsn_ = unreplicated_lsn;
            }
        }
        
        for (uint64_t log_buf_id = 0; log_buf_id < g_log_buffer_num; log_buf_id++)
        {
            //该logbuffer不是该复制线程负责
            if (log_buf_id % g_replica_thread_num != (thread_id_ - replica_thread_id_offset))
                continue;
            
            LogLSN replicated_lsn = UINT64_MAX;

            for (uint64_t backup_id = 0; backup_id < g_backup_cnt; backup_id++)
            {
                log_replica_comm = g_comm_manager->log_replica_comms_[log_buf_id * g_backup_cnt + backup_id];

                if (replicated_lsn > log_replica_comm->replicated_lsn_)
                    replicated_lsn = log_replica_comm->replicated_lsn_;
            }

            log_buf = g_log_manager->GetLogBuffer(log_buf_id);
            log_buf->replicated_lsn_ = replicated_lsn;
        }
    }

    
}
