#include "replicate_thread.h"

#include "global.h"

#include "comm_manager.h"
#include "log_replica_comm.h"

#include "log_manager.h"
#include "log_buffer.h"


ReplicateThread::ReplicateThread(ThreadID thread_id, ThreadID replicate_thread_id, ProcID process_id)
{
    thread_type_  = DBThreadType::REPLICATE_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = process_id;

    replicate_thread_id_ = replicate_thread_id;
}

ReplicateThread::~ReplicateThread()
{
}



void ReplicateThread::Run()
{
#if THREAD_BIND_CORE
        SetAffinity();
#endif

    LogReplicaComm* log_replica_comm = nullptr;
    LogBuffer*      log_buf          = nullptr;

    while (g_system_state != SystemState::FINISH_STATE)
    {
        for (uint64_t i = 0; i < g_comm_manager->log_replica_comm_cnt_; i++)
        {
            if (i % g_replica_thread_num != thread_id_ - replica_thread_id_offset)
                continue;
            
            log_replica_comm = g_comm_manager->log_replica_comms_[i];
            log_buf          = log_replica_comm->backup_log_buf_;
            
            /** 超过回收日志同步的阈值
             *  需要向主节点发送同步信息 **/
        #if  SYN_RECLAIMED_LOG
        //   #if   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
        //     if (log_buf->persistented_lsn_ - log_replica_comm->syned_free_lsn_ >= g_syn_free_lsn_threshold)
        //         log_replica_comm->SynFreeLsn(log_buf->persistented_lsn_);
        //     // if (log_buf->reclaimed_lsn_ - log_replica_comm->syned_free_lsn_ >= g_syn_free_lsn_threshold)
        //     //     log_replica_comm->SynFreeLsn(log_buf->reclaimed_lsn_);
        //   #elif AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
        //     if (log_buf->persistented_lsn_ - log_replica_comm->syned_free_lsn_ >= g_syn_free_lsn_threshold)
        //         log_replica_comm->SynFreeLsn(log_buf->persistented_lsn_);
        //   #endif
            if (log_buf->free_lsn_ - log_replica_comm->syned_free_lsn_ >= g_syn_free_lsn_threshold)
                log_replica_comm->SynFreeLsn(log_buf->free_lsn_);


        #endif
            
            /* 接收主节点复制的日志 */
            log_replica_comm->ProcessRecvWC();
            
            /* 处理send wr执行完成后生成的WC */
            log_replica_comm->ProcessSendWC();
        }
    }
    
}
