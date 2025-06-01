#ifndef  LOG_REPLICA_COMM_H_
#define  LOG_REPLICA_COMM_H_

#include "config.h"
#include "db_comm.h"
#include "rdma_meta.h"


#include <queue>


class LogBuffer;
class RdmaQP;


enum LogReplicaWRType
{
    LOG_REPLICA_WR  = 0,
    RECV_MESSAGE_WR
};


class LogReplicaWR : public WorkRequest
{
public:
    uint32_t replica_log_size_;

    void set(){};
};


class RecvMessageWR : public WorkRequest
{
public:
    uint16_t slot_idx;

    void set(){};
};


enum LogReplicaMessageType
{
    SYN_RECLAIMED_LSN_MESSAGE = 0,
};


class SynReclaimedLsnMessage : public MessageMeta
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


    /**** 参与通信的日志缓冲区和备节点id ****/
    LogBufID log_buf_id_;
    uint64_t backup_id_;

    //参与复制的日志缓冲区
    LogBuffer* primary_log_buf_;


    /* 
     * 以日志缓冲区的首地址和大小构造rdma mr
     * 存放本地日志缓冲区和备节点日志缓冲区的首地址和大小
     */
    RWMRMeta local_log_buf_mr_meta_;
    RWMRMeta remote_log_buf_mr_meta_;

    struct ibv_mr* local_log_buf_mr_;


    /* 
     * 已经复制到备节点的日志的lsn。
     * 区间 [replication_lsn_, source_log_buf_->filled_lsn_)，是日志缓冲区中
     * 还未向该备节点复制的日志
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN replicated_lsn_;

    alignas(CACHE_LINE_SIZE) volatile LogLSN replicating_lsn_;
    
    /* 
     * 当前实现中，备节点日志持久化并回放后，日志并没有被回收。
     * 只读查询可能会访问日志，获取查询数据。因此，这部分日志不能被覆盖。
     * 当只读查询结束，日志不可能被新的查询访问时，才允许将日志回收，释放空闲空间。
     * 
     * reclaimed_lsn表示已经被回收的日志最大lsn。[0, reclaimed_lsn_)表示已回收日志。
     * 区间 [reclaimed_lsn_, replication_lsn），表示已经被复制到备节点，但还未被回收的日志。
     * 
     * 当存在新的日志需要复制到备节点时，需要首先根据reclaimed_lsn_确定备节点日志缓冲区是否
     * 存在足够空闲空间接收日志。
     */
    alignas(CACHE_LINE_SIZE) volatile LogLSN reclaimed_lsn_;

    
    /** send/recv 操作消息缓冲池 **/
    SRMRMeta send_mr_meta_;
    SRMRMeta recv_mr_meta_;

    struct ibv_mr* send_mr_;
    struct ibv_mr* recv_mr_;


    /* 本地qp的关键属性，发送到备节点，用于备节点qp的初始化与连接 */
    QPMeta   local_qp_meta_;
    /* 与远端相连QP的属性，用于初始化QP工作状态 */
    QPMeta   remote_qp_meta_;

    /*** 与备节点对应qp连接，进行日志复制、消息传递同步的rdma qp ***/
    RdmaQP*  log_replica_qp_;


    /*
     * 已post到rdma，但还未收到wc的，处于工作状态的wr数量
     * 要保证 outstanding_wr_ <= g_max_qp_depth;
     */
    alignas(CACHE_LINE_SIZE) volatile uint64_t outstanding_wr_;


    std::queue<WorkRequest*> send_wr_queue_;
    std::queue<WorkRequest*> recv_wr_queue_;


public:
    LogReplicaComm(LogBuffer* log_buf, uint64_t backup_id, 
                   struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                   struct ibv_pd* pd = nullptr);
    ~LogReplicaComm();

    void RegisterWithRemoteQP();

    void ReplicateLog(MemPtr send_addr, MemPtr recv_addr, uint32_t size);
   
    void AddRecvMessageWR();

    void ProcessSendWC();
    
    void ProcessRecvWC();

    void ParseAndProcessMessage(uint16_t slot_num);

};



#endif