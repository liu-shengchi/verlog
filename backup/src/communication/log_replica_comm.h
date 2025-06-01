#ifndef  LOG_REPLICA_COMM_H_
#define  LOG_REPLICA_COMM_H_

#include "config.h"
#include "db_comm.h"
#include "rdma_meta.h"


#include <queue>
#include <infiniband/verbs.h>


class LogBuffer;
class RdmaQP;


enum LogReplicaWRType
{
    LOG_REPLICA_RECV_WR  = 0,
    SYN_FREE_LSN_WR
};


class LogReplicaRecvWR : public WorkRequest
{
public:
    void set(){};
};


class SynFreeLsnWR : public WorkRequest
{
public:
    LogLSN syned_free_lsn_;

    void set(){};
};



enum LogReplicaMessageType
{
    SYN_RECLAIMED_LSN_MESSAGE = 0,
};


class SynFreeLsnMessage : public MessageMeta
{
public:
    LogLSN reclaimed_lsn_;

    void set(){};
};





class LogReplicaComm : public DBComm
{
private:

    /**** 友元类 ****/
    friend class ReplicateThread;


    /**** 参与通信的日志缓冲区id ****/
    LogBufID log_buf_id_;

    //参与复制的日志缓冲区
    LogBuffer* backup_log_buf_;


    //
    RWMRMeta local_log_buf_mr_meta_;
    RWMRMeta remote_log_buf_mr_meta_;

    struct ibv_mr* local_log_buf_mr_;

    SRMRMeta send_mr_meta_;
    SRMRMeta recv_mr_meta_;

    struct ibv_mr* send_mr_;
    struct ibv_mr* recv_mr_;


    QPMeta   local_qp_meta_;
    /* 与远端相连QP的属性，用于初始化QP工作状态 */
    QPMeta   remote_qp_meta_;

    //
    RdmaQP*  log_replica_qp_;


    std::queue<WorkRequest*> send_wr_queue_;
    std::queue<WorkRequest*> recv_wr_queue_;


    /* 
     * 已经向主节点同步的已回收的lsn。
     * 对主节点来说，区间 [syned_free_lsn_, lsn_)，是日志缓冲区中还未回收的日志。
     * 对备节点来说，[syned_free_lsn_, log_buffer->reclaimed_lsn_)表示备节点已经回收，
     * 但还未同步到主节点。当区间超过阈值时，需要由复制线程同步reclaimed_lsn_。
     */
    LogLSN syned_free_lsn_;

    void AddRecvIMMWR();
    void AddSendMessageWR();


public:
    LogReplicaComm(LogBuffer* log_buf, 
                   struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                   struct ibv_pd* pd = nullptr);
    ~LogReplicaComm();

    void RegisterWithRemoteQP();
    
    void SynFreeLsn(LogLSN free_lsn);
    
    void ProcessSendWC();
    
    void ProcessRecvWC();

};



#endif