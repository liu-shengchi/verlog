#ifndef COMMUNICATION_DB_COMM_H_
#define COMMUNICATION_DB_COMM_H_

#include "config.h"

#include "rdma_meta.h"

#include "infiniband/verbs.h"



class DBComm
{
protected:
     
    int CreateSocketListen();
    int AcceptSocketConnect(int sock);
    int ConnectSocket(std::string remote_ip, uint32_t remote_port);


    /* rdma设备信息 */
    struct ibv_context*    device_ctx_;
    uint32_t               rdma_port_;   /* 用于rdma查询的端口号: ibv_query_port */
    struct ibv_port_attr*  port_attr_;

    /* 
     * 通信所需rdma组件所在的保护域 
     */
    struct ibv_pd* pd_;                     /* Protection Domain handler */


public:

    virtual void RegisterWithRemoteQP() = 0;
    // virtual void GetLocalMRMetaDta(MRMeta& mr_meta) = 0;
};



#endif