#include "comm_manager.h"

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

    ibv_query_device(device_ctx_, &device_attr_);

    // printf("max_mr_size = %lu\n", device_attr_.max_mr_size);

    /* 获取rdma port端口号、端口属性、GID */
    rdma_port_ = 1;
    ibv_query_port(device_ctx_, rdma_port_, &port_attr_);
    ibv_query_gid(device_ctx_, rdma_port_, 0, &local_gid_);


    backup_cnt_ = g_backup_cnt;

    /* 创建日志缓冲区主备复制通信管理 */
    log_buffer_cnt_ = g_log_buffer_num;
    log_replica_comm_cnt_ = log_buffer_cnt_ * backup_cnt_;
    log_replica_comms_ = new LogReplicaComm*[log_replica_comm_cnt_];
    
    for (uint64_t log_buf_id = 0; log_buf_id < log_buffer_cnt_; log_buf_id++)
    {
        for (uint64_t backup_id = 0; backup_id < backup_cnt_; backup_id++)
        {
            printf("开始创建第 %ld 个日志缓冲区向第 %ld 个备节点的日志复制通信管理!\n", log_buf_id, backup_id);
            log_replica_comms_[log_buf_id*backup_cnt_+backup_id] = 
                                        new LogReplicaComm(g_log_manager->GetLogBuffer(log_buf_id), backup_id,  
                                                           device_ctx_, rdma_port_, &port_attr_);
        }
    }

    /* 创建全局时间戳通信管理器 */
    global_ts_comm_cnt_ = backup_cnt_;
    global_ts_comms_    = new GlobalTSComm*[global_ts_comm_cnt_];

    for (uint64_t backup_id = 0; backup_id < backup_cnt_; backup_id++)
    {
        printf("开始创建第 %ld 个备节点的时间戳通信同步管理!\n", backup_id);
        global_ts_comms_[backup_id] = new GlobalTSComm(backup_id, device_ctx_, rdma_port_, &port_attr_);
    }

    /* 创建系统控制通信管理器 */
    sys_control_comm_cnt_ = backup_cnt_;
    sys_control_comms_    = new SysControlComm*[sys_control_comm_cnt_];

    for (uint64_t backup_id = 0; backup_id < backup_cnt_; backup_id++)
    {
        printf("开始创建第 %ld 个备节点的系统控制通信管理!\n", backup_id);
        sys_control_comms_[backup_id] = new SysControlComm(backup_id, device_ctx_, rdma_port_, &port_attr_);
    }

}

CommManager::~CommManager()
{

}


void CommManager::ConnectWithRemote()
{
    // for (uint64_t log_buf_id = 0; log_buf_id < log_buffer_cnt_; log_buf_id++)
    // {
    //     for (uint64_t backup_id = 0; backup_id < backup_cnt_; backup_id++)
    //     {
    //         printf("\n开始为第 %ld 个日志缓冲区与第 %ld 个备节点建立日志复制通信连接!\n", log_buf_id, backup_id);
    //         log_replica_comms_[log_buf_id*backup_cnt_+backup_id]->RegisterWithRemoteQP();
    //     }
    // }

    // for (uint64_t backup_id = 0; backup_id < global_ts_comm_cnt_; backup_id++)
    // {
    //     printf("\n开始与第 %ld 个备节点建立全局时间戳通信连接!\n", backup_id);
    //     global_ts_comms_[backup_id]->RegisterWithRemoteQP();
    // }

    // for (uint64_t backup_id = 0; backup_id < sys_control_comm_cnt_; backup_id++)
    // {
    //     printf("\n开始与第 %ld 个备节点建立系统控制通信连接!\n", backup_id);
    //     sys_control_comms_[backup_id]->RegisterWithRemoteQP();
    // }

    for (uint64_t backup_id = 0; backup_id < backup_cnt_; backup_id++)
    {
        for (uint64_t log_buf_id = 0; log_buf_id < log_buffer_cnt_; log_buf_id++)
        {
            printf("\n开始为第 %ld 个日志缓冲区与第 %ld 个备节点建立日志复制通信连接!\n", log_buf_id, backup_id);
            log_replica_comms_[log_buf_id*backup_cnt_+backup_id]->RegisterWithRemoteQP();
        }

        printf("\n开始与第 %ld 个备节点建立全局时间戳通信连接!\n", backup_id);
        global_ts_comms_[backup_id]->RegisterWithRemoteQP();

        printf("\n开始与第 %ld 个备节点建立系统控制通信连接!\n", backup_id);
        sys_control_comms_[backup_id]->RegisterWithRemoteQP();
    }

    // for (uint64_t backup_id = 0; backup_id < global_ts_comm_cnt_; backup_id++)
    // {
    //     printf("\n开始与第 %ld 个备节点建立全局时间戳通信连接!\n", backup_id);
    //     global_ts_comms_[backup_id]->RegisterWithRemoteQP();
    // }

    // for (uint64_t backup_id = 0; backup_id < sys_control_comm_cnt_; backup_id++)
    // {
    //     printf("\n开始与第 %ld 个备节点建立系统控制通信连接!\n", backup_id);
    //     sys_control_comms_[backup_id]->RegisterWithRemoteQP();
    // }
}


SysControlComm* CommManager::GetSysControlComm(uint64_t backup_id)
{
    return sys_control_comms_[backup_id];
}
