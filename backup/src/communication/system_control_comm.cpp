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


SysControlComm::SysControlComm(struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                               struct ibv_pd* pd)
{
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
     * 在当前的设计中，备节点只需要接收主节点发送系统控制信息，改变备节点状态。
     * 因此只需要创建接收消息池 & recv_mr即可。
     */
    send_mr_meta_.message_pool_      = 0;
    send_mr_meta_.message_pool_size_ = 0;
    send_mr_ = nullptr;
    
    recv_mr_meta_.slot_cnt_  = g_qp_max_depth;
    recv_mr_meta_.slot_size_ = g_max_message_size;
    recv_mr_meta_.message_pool_size_ = recv_mr_meta_.slot_cnt_ * recv_mr_meta_.slot_size_;
    recv_mr_meta_.message_pool_ = reinterpret_cast<MemPtr>(malloc(recv_mr_meta_.message_pool_size_));
    recv_mr_meta_.first_free_slot_ = 0;
    recv_mr_meta_.first_used_slot_ = 0;
    recv_mr_meta_.is_full_         = false;
    
    unsigned int mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    recv_mr_ = ibv_reg_mr(pd_, (void*)recv_mr_meta_.message_pool_, recv_mr_meta_.message_pool_size_, mr_access_type);

    recv_mr_meta_.lkey = recv_mr_->lkey;


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
    
    printf("向主节点发送系统控制的 qp 信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \n", 
            local_qp_meta_.qp_num, local_qp_meta_.port_lid, local_qp_meta_.port_num);

    //offset == register_info_size


    /* 和远端服务器建立socket连接，交换 qp mr 信息 */
    //远端服务器ip port
    std::string remote_ip   = primary_ip;
    uint32_t    remote_port = primary_port;

    printf("与第主节点建立socket连接!\n");

    //建立socket连接
    int recv_sock = 0;
    int recv_fd   = 0;
    recv_sock = CreateSocketListen();
    recv_fd   = AcceptSocketConnect(recv_sock);
    recv(recv_fd, remote_register_info, remote_info_size, 0);

    printf("与主节点交换qp信息!\n");

    int send_sock = 0;
    send_sock = ConnectSocket(remote_ip, remote_port);
    send(send_sock, local_register_info, local_info_size, 0);

    close(recv_sock);
    close(recv_fd);
    close(send_sock);
    printf("与主节点关闭socket连接!\n");    

    /* 解析远端qp的信息 */
    QPMeta remote_qp_meta;

    offset = 0;
    memcpy((char*)&(remote_qp_meta_.qp_num), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_qp_meta_.port_lid), remote_register_info + offset, 2);
    offset += 2;
    memcpy((char*)&(remote_qp_meta_.port_num), remote_register_info + offset, 1);
    offset += 1;

    printf("从主节点接收的qp信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \n", 
            remote_qp_meta_.qp_num, remote_qp_meta_.port_lid, remote_qp_meta_.port_num);

    /* 在本地系统控制通信管理中，设置远端qp的信息 */
    sys_control_qp_->SetRmoteQPMetaData(remote_qp_meta_);
    //初始化qp状态,使其能够对外进行服务
    sys_control_qp_->MofifyQPToReady();

    /* 预先放置获取主节点系统控制信息的recv wr*/
    for (int i = 0; i < 32; i++)
    {
        AddRecvMessageWR();
    }


}


int SysControlComm::ProcessRecvWC()
{
    struct ibv_wc wc;
    
    int ret = 0;

    ret = sys_control_qp_->PullWCFromRecvCQ(&wc);    
    if (ret == 0)
    {
        // printf("未获取到cq状态\n");
    }
    else if (ret == 1)
    {
        if( wc.status != IBV_WC_SUCCESS )
        {
            printf("获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
        }
        else
        {
            printf("成功获取cq: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
            uint16_t first_used_slot = recv_mr_meta_.first_used_slot_;
            ParseAndProcessMessage(first_used_slot);
            recv_mr_meta_.first_used_slot_ = (first_used_slot + 1) % recv_mr_meta_.slot_cnt_;
            recv_mr_meta_.is_full_ = false;
        }

        AddRecvMessageWR();
    }
    else
    {
        printf("日志复制过程中出现错误!\n");
    }
    
    return ret;
}


void SysControlComm::AddRecvMessageWR()
{
    if (recv_mr_meta_.is_full_)
        return;
    
    sys_control_qp_->PostRecvWR(recv_mr_meta_.message_pool_ + recv_mr_meta_.first_free_slot_ * recv_mr_meta_.slot_size_, 
                                recv_mr_meta_.slot_size_, recv_mr_meta_.lkey);
    
    recv_mr_meta_.first_free_slot_ = (recv_mr_meta_.first_free_slot_ + 1) % recv_mr_meta_.slot_cnt_;

    if (recv_mr_meta_.first_free_slot_ == recv_mr_meta_.first_used_slot_)
        recv_mr_meta_.is_full_ = true;
}


void SysControlComm::ParseAndProcessMessage(uint16_t slot_num)
{
    MemPtr  start_ptr = recv_mr_meta_.message_pool_ + slot_num * recv_mr_meta_.slot_size_;
    int     offset    = 0;

    uint8_t message_type = 0;
    message_type = *(uint8_t*)(start_ptr + offset);
    offset      += sizeof(message_type);

    switch ((SysControlMessageType)message_type)
    {
    case SYS_STATE_SWITCH_MESSAGE:
        {
            uint8_t sys_state = *(uint8_t*)(start_ptr + offset);
            g_system_state = (SystemState)sys_state;
            // printf("系统状态改变: %ld\n", sys_state);
            break;
        }
        
    default:
        break;
    }
}
