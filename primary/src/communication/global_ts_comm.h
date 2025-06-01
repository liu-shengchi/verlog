#ifndef COMMUNICATION_GLOBAL_TS_H_
#define COMMUNICATION_GLOBAL_TS_H_

#include "config.h"

#include "db_comm.h"



class RdmaQP;


class GlobalTSComm : public DBComm
{
private:

    uint64_t backup_id_;

    /** 主节点全局时间戳mr **/
    RWMRMeta       local_global_ts_mr_meta_;
    struct ibv_mr* global_ts_mr_;


    QPMeta   local_qp_meta_;
    QPMeta   remote_qp_meta_;

    /** 用于全局事务时间戳同步的RDMA QP **/
    RdmaQP*  global_ts_qp_;


public:
    GlobalTSComm(uint64_t backup_id, 
                 struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                 struct ibv_pd* pd = nullptr);
    ~GlobalTSComm();

    void RegisterWithRemoteQP();

};


#endif