#include "comm_manager.h"

#include "global.h"

#include "log_replica_comm.h"
#include "global_ts_comm.h"
#include "system_control_comm.h"

#include "log_manager.h"
#include "log_buffer.h"


CommManager::CommManager()
{
    int                 device_num = 0;
    struct ibv_device** device_list = nullptr;

    /* 获取RDMA设备和上下文 */
    device_list = ibv_get_device_list(&device_num);
    device_     = device_list[0];
    device_ctx_ = ibv_open_device(device_);

    ibv_free_device_list(device_list);

    /* 获取rdma port端口号、端口属性、GID */
    rdma_port_ = 1;
    ibv_query_port(device_ctx_, rdma_port_, &port_attr_);
    ibv_query_gid(device_ctx_, rdma_port_, 0, &local_gid_);


    /* 创建日志缓冲区主备复制通信管理 */
    log_replica_comm_cnt_ = g_log_buffer_num;
    log_replica_comms_    = new LogReplicaComm*[log_replica_comm_cnt_];
    
    for (uint64_t log_buf_id = 0; log_buf_id < log_replica_comm_cnt_; log_buf_id++)
    {
        printf("开始创建第 %ld 个日志缓冲区与主节点的日志复制通信管理!\n", log_buf_id);
        LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);
        log_replica_comms_[log_buf_id] = 
                    new LogReplicaComm(g_log_manager->GetLogBuffer(log_buf_id), 
                                       device_ctx_, rdma_port_, &port_attr_);
    }

    /* 创建全局时间戳通信管理器 */
    printf("开始创建与主节点的时间戳通信同步管理!\n");
    global_ts_comm_ = new GlobalTSComm(device_ctx_, rdma_port_, &port_attr_);

    printf("开始创建与主节点的系统控制通信同步管理!\n");
    sys_control_comm_ = new SysControlComm(device_ctx_, rdma_port_, &port_attr_);


}

CommManager::~CommManager()
{

}


void CommManager::ConnectWithRemote()
{
    for (uint64_t log_buf_id = 0; log_buf_id < log_replica_comm_cnt_; log_buf_id++)
    {
        printf("\n开始为第 %ld 个日志缓冲区与主节点建立通信连接!\n", log_buf_id);
        log_replica_comms_[log_buf_id]->RegisterWithRemoteQP();
    }
    
    printf("\n开始与主节点建立全局时间戳建立通信连接!\n");
    global_ts_comm_->RegisterWithRemoteQP();

    printf("\n开始与主节点建立系统控制建立通信连接!\n");
    sys_control_comm_->RegisterWithRemoteQP();
}


SysControlComm* CommManager::GetSysControlComm()
{
    return sys_control_comm_;
}
