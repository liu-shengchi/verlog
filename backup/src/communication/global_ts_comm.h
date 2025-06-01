#ifndef COMMUNICATION_GLOBAL_TS_H_
#define COMMUNICATION_GLOBAL_TS_H_

#include "config.h"

#include "db_comm.h"

#include "infiniband/verbs.h"
#include <mutex>


class RdmaQP;


class GlobalTSComm : public DBComm
{
private:

    /**** 友元类 ****/
    friend class TxnThread;

    uint64_t backup_id_;

    /** 主节点全局时间戳mr **/
    RWMRMeta       local_global_ts_mr_meta_;
    RWMRMeta       remote_global_ts_mr_meta_;
    struct ibv_mr* global_ts_mr_;


    QPMeta   local_qp_meta_;
    QPMeta   remote_qp_meta_;

    /** 用于全局事务时间戳同步的RDMA QP **/
    RdmaQP*  global_ts_qp_;

    
    /* 
     * 实现PolarDB-SCC提出的 Linear Lamport timestamp 机制，论文中只介绍了原理，
     * 没有给出具体实现。
     * 论文: PolarDB-SCC: A Cloud-Native Database Ensuring Low Latency
     *       for Strongly Consistent Reads, VLDB, 2023
     * 
     * ts_comm_mutex_ 用于封锁。保证同一时刻只有一个事务向主节点发送获取时间戳请求
     * fetching_cts_  表示是否正在获取时间戳，false表示不在获取时间戳时间内
     *                                      true表示正在获取时间戳
     *                
     * 
     * latest_arrive_ct_表示最近一次从主节点获取g_commit_ts的只读查询到达备节点时的时间戳
     * max_fetching_arrive_ct_表示目前等待获取时间戳的事务中，最大的arrive_ts
     */
    std::mutex         ts_comm_mutex_;
    volatile bool      fetching_cts_;

    volatile ClockTime latest_fetched_arrive_ct_;
    volatile ClockTime max_fetching_arrive_ct_;


public:
    GlobalTSComm(struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                 struct ibv_pd* pd = nullptr);
    ~GlobalTSComm();

    void RegisterWithRemoteQP();

    Timestamp FetchGlobalCTS(ClockTime arrive_ct);
};


#endif