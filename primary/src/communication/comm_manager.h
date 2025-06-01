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
    

    /* RDMA设备基本信息 */
    struct ibv_device*     device_;
    struct ibv_context*    device_ctx_;
    struct ibv_device_attr device_attr_;

    uint32_t               rdma_port_;  /* 用于rdma查询的端口号: ibv_query_port */
    struct ibv_port_attr   port_attr_;

    union  ibv_gid         local_gid_;  /* 用于跨RDMA子网通信，目前系统不需要 */


    uint64_t backup_cnt_;
    uint32_t log_buffer_cnt_;

    /* 
     * 为每个日志缓冲去创建一个日志复制管理器
     * 以日志缓冲区和备节点为单位，同步、管理主备日志复制
     * backup_cnt_ * log_buffer_cnt_
     */
    uint64_t         log_replica_comm_cnt_;
    LogReplicaComm** log_replica_comms_;

    /* 
     * 为每个备节点创建 g_commit_ts 时间戳通信同步管理器 
     */
    uint64_t         global_ts_comm_cnt_;
    GlobalTSComm**   global_ts_comms_;

    /* 
     * 为每个备节点创建 g_commit_ts 时间戳通信同步管理器 
     */
    uint64_t         sys_control_comm_cnt_;
    SysControlComm** sys_control_comms_;


public:
    CommManager();
    ~CommManager();

    void ConnectWithRemote();

    SysControlComm* GetSysControlComm(uint64_t backup_id);

};


#endif