#include "system_control_comm.h"

#include "rdma_qp.h"


#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <iostream>


SysControlComm::SysControlComm(uint64_t backup_id, 
                               struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                               struct ibv_pd* pd)
{
    backup_id_  = backup_id;

    device_ctx_ = device_ctx;
    rdma_port_  = rdma_port;
    port_attr_  = port_attr;

    if (pd == nullptr)
    {
        /* 创建新的 Protection Domain */
        pd_ = ibv_alloc_pd(device_ctx_);
        if (!pd_) {
            printf("创建pd失败! \n");
        }
    }
    else
    {
        pd_ = pd;
    }

    /* 
     * 初始化用于send/recv消息传递的message pool 和 mr。
     * 在当前的设计中，主节点只需要向备节点发送系统控制信息，
     * 因此只需要创建发送消息池 & send_mr即可。
     */
    recv_mr_meta_.message_pool_      = 0;
    recv_mr_meta_.message_pool_size_ = 0;
    recv_mr_ = nullptr;
    

    send_mr_meta_.slot_cnt_  = g_qp_max_depth;
    send_mr_meta_.slot_size_ = g_max_message_size;
    send_mr_meta_.message_pool_size_ = send_mr_meta_.slot_cnt_ * send_mr_meta_.slot_size_;
    send_mr_meta_.message_pool_ = reinterpret_cast<MemPtr>(malloc(send_mr_meta_.message_pool_size_));
    send_mr_meta_.first_free_slot_ = 0;
    send_mr_meta_.first_used_slot_ = 0;
    send_mr_meta_.is_full_         = false;
    
    unsigned int mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE;
    send_mr_ = ibv_reg_mr(pd_, (void*)send_mr_meta_.message_pool_, send_mr_meta_.message_pool_size_, mr_access_type);

    send_mr_meta_.lkey = send_mr_->lkey;

    /* 
     * 初始化 rdma qp
     * 还未成功建立QP间的连接，只是初始化了数据结构
     */
    sys_control_qp_ = new RdmaQP(nullptr, send_mr_, recv_mr_, 
                                 device_ctx_, rdma_port_, port_attr_, pd_);

    sys_control_qp_->GetLocalQPMetaData(local_qp_meta_);

}

SysControlComm::~SysControlComm()
{
}


void SysControlComm::RegisterWithRemoteQP()
{
    /* QPMeta(4+2+1) */
    uint32_t local_info_size  = 7;
    /* QPMeta(4+2+1) */
    uint32_t remote_info_size = 7;
    char*    local_register_info  = (char*)malloc(local_info_size);
    char*    remote_register_info = (char*)malloc(remote_info_size);

    /* 
     * 获取本地 qp 的访问信息
     */
    int offset = 0;

    memcpy(local_register_info + offset, &(local_qp_meta_.qp_num), 4);
    offset += 4;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_lid), 2);
    offset += 2;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_num), 1);
    offset += 1;
    
    printf("向第 %ld 个备节点发送系统控制的 qp 信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \n", 
            backup_id_, local_qp_meta_.qp_num, local_qp_meta_.port_lid, local_qp_meta_.port_num);

    //offset == register_info_size


    /* 和远端服务器建立socket连接，交换 qp mr 信息 */
    //远端服务器ip port
    std::string remote_ip   = backup_ip_map.at(backup_id_);
    uint32_t    remote_port = backup_port_map.at(backup_id_);

    printf("与第 %ld 个备节点建立socket连接!\n", backup_id_);

    //建立socket连接
    int send_sock = 0;
    send_sock = ConnectSocket(remote_ip, remote_port);
    send(send_sock, local_register_info, local_info_size, 0);

    printf("与第 %ld 个备节点交换qp mr信息!\n", backup_id_);

    int recv_sock = 0;
    int recv_fd   = 0;
    recv_sock = CreateSocketListen();
    recv_fd   = AcceptSocketConnect(recv_sock);
    recv(recv_fd, remote_register_info, remote_info_size, 0);

    close(recv_sock);
    close(recv_fd);
    close(send_sock);
    printf("与第 %ld 个备节点关闭socket连接!\n", backup_id_);    

    /* 解析远端qp的信息 */
    QPMeta remote_qp_meta;

    offset = 0;
    memcpy((char*)&(remote_qp_meta_.qp_num), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_qp_meta_.port_lid), remote_register_info + offset, 2);
    offset += 2;
    memcpy((char*)&(remote_qp_meta_.port_num), remote_register_info + offset, 1);
    offset += 1;

    printf("从第 %ld 个备节点接收的qp信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \n", 
        backup_id_, remote_qp_meta_.qp_num, remote_qp_meta_.port_lid, remote_qp_meta_.port_num);

    /* 在本地系统控制通信管理中，设置远端qp的信息 */
    sys_control_qp_->SetRmoteQPMetaData(remote_qp_meta_);
    //初始化qp状态,使其能够对外进行服务
    sys_control_qp_->MofifyQPToReady();

}


void SysControlComm::AddSendMessageWR()
{
    if (send_mr_meta_.is_full_)
        return;
    
    printf("添加控制信息wr!\n");

    sys_control_qp_->PostSendWR(send_mr_meta_.message_pool_ + send_mr_meta_.first_free_slot_ * send_mr_meta_.slot_size_, 
                                send_mr_meta_.slot_size_, send_mr_meta_.lkey);

    send_mr_meta_.first_free_slot_ = (send_mr_meta_.first_free_slot_ + 1) % send_mr_meta_.slot_cnt_;

    if (send_mr_meta_.first_free_slot_ == send_mr_meta_.first_used_slot_)
        send_mr_meta_.is_full_ = true;
}


void SysControlComm::SystemStateSwitch(SystemState system_state)
{
    if (send_mr_meta_.is_full_)
    {
        printf("rdma send队列已满!\n");
        return;
    }

    int slot_idx = send_mr_meta_.first_free_slot_;

    uint64_t start_ptr = send_mr_meta_.message_pool_ + slot_idx * send_mr_meta_.slot_size_;
    uint64_t offset    = 0;
    
    uint8_t message_type = (uint8_t)SysControlMessageType::SYS_STATE_SWITCH_MESSAGE;
    memcpy((char*)(start_ptr + offset), (char*)&message_type, sizeof(message_type));
    offset += sizeof(message_type);

    uint8_t sys_state = (uint8_t)system_state;

    memcpy((char*)(start_ptr + offset), (char*)&sys_state, sizeof(sys_state));
    offset += sizeof(sys_state);

    AddSendMessageWR();
}


int SysControlComm::ProcessSendWC()
{
    struct ibv_wc wc;
    WorkRequest*  wr = nullptr;

    int ret = 0;
    ret = sys_control_qp_->PullWCFromSendCQ(&wc);
    
    if (ret == 1)
    {
        if( wc.status != IBV_WC_SUCCESS )
        {
            printf("获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
        }
        else
        {
            printf("成功获取cq: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
        }
    }
    

    if (ret < 0)
        printf("发送系统控制信息的过程中出现错误!\n");

    return ret;
}

