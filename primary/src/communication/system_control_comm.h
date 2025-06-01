#ifndef SYSTEM_CONTROL_COMM_H_
#define SYSTEM_CONTROL_COMM_H_

#include "config.h"
#include "global.h"

#include "db_comm.h"
#include "rdma_meta.h"


class RdmaQP;



enum SysControlMessageType
{
    SYS_STATE_SWITCH_MESSAGE = 0,
};


class SystemStateSwitch : public MessageMeta
{
    uint8_t system_state_;
};


class SysControlComm : public DBComm
{
private:
    
    uint64_t backup_id_;

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
    RdmaQP*  sys_control_qp_;


    void AddSendMessageWR();


public:
    SysControlComm(uint64_t backup_id, 
                   struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                   struct ibv_pd* pd = nullptr);
    ~SysControlComm();

    void RegisterWithRemoteQP();

    void SystemStateSwitch(SystemState system_state);

    int ProcessSendWC();

};





#endif