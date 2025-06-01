#include "global_ts_comm.h"

#include "db_comm.h"

#include "rdma_qp.h"

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>


GlobalTSComm::GlobalTSComm(uint64_t backup_id, 
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


    //set mr access rights
    unsigned int mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    
    global_ts_mr_ = ibv_reg_mr(pd_, &g_commit_ts, sizeof(Timestamp), mr_access_type);

    if (global_ts_mr_ == nullptr) {
        printf("为global ts创建mr失败! \n");
    }

    local_global_ts_mr_meta_.mr_addr = reinterpret_cast<MemPtr>(global_ts_mr_->addr);
    local_global_ts_mr_meta_.mr_size = global_ts_mr_->length;
    local_global_ts_mr_meta_.lkey    = global_ts_mr_->lkey;
    local_global_ts_mr_meta_.rkey    = global_ts_mr_->rkey;

    /* 
     * 初始化 rdma qp
     * 还未成功建立QP间的连接，只是初始化了数据结构
     */
    global_ts_qp_ = new RdmaQP(global_ts_mr_, nullptr, nullptr, device_ctx_, rdma_port_, port_attr_, pd_);

    global_ts_qp_->GetLocalQPMetaData(local_qp_meta_);

}

GlobalTSComm::~GlobalTSComm()
{

}


void GlobalTSComm::RegisterWithRemoteQP()
{
    /* QPMeta(4+2+1) + MRMeta(8+8+4+4) */
    uint32_t local_info_size  = 31;
    /* QPMeta(4+2+1) */
    uint32_t remote_info_size = 7;
    char*    local_register_info  = (char*)malloc(local_info_size);
    char*    remote_register_info = (char*)malloc(remote_info_size);

    /* 
        * 获取本地 qp 和 mr 的访问信息
        */
    int offset = 0;

    memcpy(local_register_info + offset, &(local_qp_meta_.qp_num), 4);
    offset += 4;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_lid), 2);
    offset += 2;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_num), 1);
    offset += 1;
    
    memcpy(local_register_info + offset, &(local_global_ts_mr_meta_.mr_addr), 8);
    offset += 8;
    memcpy(local_register_info + offset, &(local_global_ts_mr_meta_.mr_size), 8);
    offset += 8;
    memcpy(local_register_info + offset, &(local_global_ts_mr_meta_.lkey), 4);
    offset += 4;
    memcpy(local_register_info + offset, &(local_global_ts_mr_meta_.rkey), 4);
    offset += 4;
    
    printf("向第 %ld 个备节点发送全局时间戳的qp mr信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \nmr_addr: %ld \nmr_size: %ld \nlkey: %ld \nrkey: %ld \n", 
            backup_id_, local_qp_meta_.qp_num, local_qp_meta_.port_lid, local_qp_meta_.port_num, 
            local_global_ts_mr_meta_.mr_addr, local_global_ts_mr_meta_.mr_size, local_global_ts_mr_meta_.lkey, local_global_ts_mr_meta_.rkey);

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

    /* 在本地全局时间戳通信管理中，设置远端qp的信息 */
    global_ts_qp_->SetRmoteQPMetaData(remote_qp_meta_);
    //初始化qp状态,使其能够对外进行服务
    global_ts_qp_->MofifyQPToReady();

}

