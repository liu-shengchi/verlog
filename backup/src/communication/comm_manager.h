#ifndef COMMUNICATION_COMM_MANAGER_H_
#define COMMUNICATION_COMM_MANAGER_H_s

#include "config.h"

#include <infiniband/verbs.h>


class LogReplicaComm;
class GlobalTSComm;
class SysControlComm;


class CommManager
{
private:
    
    /*** 友元类 ***/
    friend class ReplicateThread;
    friend class TxnThread;
    friend class TxnContext;

    /* RDMA设备基本信息 */
    struct ibv_device*     device_;
    struct ibv_context*    device_ctx_;

    uint32_t               rdma_port_;  /* 用于rdma查询的端口号: ibv_query_port */
    struct ibv_port_attr   port_attr_;

    union  ibv_gid         local_gid_;  /* 用于跨RDMA子网通信，目前系统不需要 */

    /* 
     * 为每个日志缓冲去创建一个日志复制管理器
     * 以日志缓冲区为单位，同步、管理主备日志复制
     */
    uint32_t         log_replica_comm_cnt_;
    LogReplicaComm** log_replica_comms_;

    /* 
     * 为每个备节点创建 g_commit_ts 时间戳通信同步管理器 
     */
    GlobalTSComm*    global_ts_comm_;

    /* 
     * 为每个备节点创建 系统控制 通信管理器
     */
    SysControlComm*  sys_control_comm_;



public:
    CommManager();
    ~CommManager();

    void ConnectWithRemote();
    
    SysControlComm* GetSysControlComm();
};


#endif