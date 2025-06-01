#include "log_replica_comm.h"

#include "log_buffer.h"
#include "rdma_qp.h"


#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <iostream>


LogReplicaComm::LogReplicaComm(LogBuffer* log_buf, uint64_t backup_id, 
                               struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
                               struct ibv_pd* pd)
{
    log_buf_id_ = log_buf->log_buffer_id_;
    backup_id_  = backup_id;

    primary_log_buf_ = log_buf;

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

    /* 为日志缓冲区分配mr */
    //set mr access rights
    unsigned int mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ;
    
    local_log_buf_mr_ = ibv_reg_mr(pd_, reinterpret_cast<void*>(log_buf->buffer_), log_buf->buffer_size_, mr_access_type);
    local_log_buf_mr_meta_.mr_addr = reinterpret_cast<MemPtr>(log_buf->buffer_);
    local_log_buf_mr_meta_.mr_size = log_buf->buffer_size_;
    local_log_buf_mr_meta_.lkey    = local_log_buf_mr_->lkey;
    local_log_buf_mr_meta_.rkey    = local_log_buf_mr_->rkey;

    if (local_log_buf_mr_ == nullptr) {
        printf("为日志缓冲区创建mr失败! \n");
    }

    //初始化日志复制相关控制参数
    replicated_lsn_  = 0;
    replicating_lsn_ = 0;
    reclaimed_lsn_   = 0;    

    /* 
     * 初始化用于send/recv消息传递的message pool 和 mr。
     * 在当前的设计中，主节点只需要接收来自备节点日志回收进度的消息
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
    
    mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    recv_mr_ = ibv_reg_mr(pd_, (void*)recv_mr_meta_.message_pool_, recv_mr_meta_.message_pool_size_, mr_access_type);

    recv_mr_meta_.lkey = recv_mr_->lkey;

    /* 
     * 初始化 rdma qp
     * 还未成功建立QP间的连接，只是初始化了数据结构
     */
    log_replica_qp_ = new RdmaQP(local_log_buf_mr_, send_mr_, recv_mr_, 
                                 device_ctx_, rdma_port_, port_attr_, pd_);

    log_replica_qp_->GetLocalQPMetaData(local_qp_meta_);

    
    outstanding_wr_ = 0;
}

LogReplicaComm::~LogReplicaComm()
{
}



void LogReplicaComm::RegisterWithRemoteQP()
{
    /* QPMeta(4+2+1) + MRMeta(8+8+4+4) */
    uint32_t register_info_size   = 31;
    char*    local_register_info  = (char*)malloc(register_info_size);
    char*    remote_register_info = (char*)malloc(register_info_size);

    /* 
     * 获取本地 qp 和 日志mr 的访问信息
     */
    uint64_t offset = 0;

    memcpy(local_register_info + offset, &(local_qp_meta_.qp_num), 4);
    offset += 4;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_lid), 2);
    offset += 2;
    memcpy(local_register_info + offset, &(local_qp_meta_.port_num), 1);
    offset += 1;
    
    memcpy(local_register_info + offset, &(local_log_buf_mr_meta_.mr_addr), 8);
    offset += 8;
    memcpy(local_register_info + offset, &(local_log_buf_mr_meta_.mr_size), 8);
    offset += 8;
    memcpy(local_register_info + offset, &(local_log_buf_mr_meta_.lkey), 4);
    offset += 4;
    memcpy(local_register_info + offset, &(local_log_buf_mr_meta_.rkey), 4);
    offset += 4;
    
    printf("向第 %ld 个备节点发送日志缓冲区 %ld 的qp mr信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \nmr_addr: %ld \nmr_size: %ld \nlkey: %ld \nrkey: %ld \n", 
            backup_id_, log_buf_id_, 
            local_qp_meta_.qp_num, local_qp_meta_.port_lid, local_qp_meta_.port_num, 
            local_log_buf_mr_meta_.mr_addr, local_log_buf_mr_meta_.mr_size, local_log_buf_mr_meta_.lkey, local_log_buf_mr_meta_.rkey);

    //offset == register_info_size

    /* 和远端服务器建立socket连接，交换 qp mr 信息 */
    //远端服务器ip port
    std::string remote_ip   = backup_ip_map.at(backup_id_);
    uint32_t    remote_port = backup_port_map.at(backup_id_);

    printf("与第 %ld 个备节点建立socket连接!\n", backup_id_);
    //建立socket连接
    int send_sock = 0;
    send_sock = ConnectSocket(remote_ip, remote_port);
    printf("1\n");
    send(send_sock, local_register_info, register_info_size, 0);
    printf("与第 %ld 个备节点交换qp mr信息!\n", backup_id_);

    int recv_sock = 0;
    int recv_fd   = 0;
    recv_sock = CreateSocketListen();
    recv_fd   = AcceptSocketConnect(recv_sock);
    recv(recv_fd, remote_register_info, register_info_size, 0);
    

    close(recv_sock);
    close(recv_fd);
    close(send_sock);

    printf("与第 %ld 个备节点关闭socket连接!\n", backup_id_);


    /* 解析远端qp 和 日志mr的信息 */

    offset = 0;
    memcpy((char*)&(remote_qp_meta_.qp_num), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_qp_meta_.port_lid), remote_register_info + offset, 2);
    offset += 2;
    memcpy((char*)&(remote_qp_meta_.port_num), remote_register_info + offset, 1);
    offset += 1;
    
    memcpy((char*)&(remote_log_buf_mr_meta_.mr_addr), remote_register_info + offset, 8);
    offset += 8;
    memcpy((char*)&(remote_log_buf_mr_meta_.mr_size), remote_register_info + offset, 8);
    offset += 8;
    memcpy((char*)&(remote_log_buf_mr_meta_.lkey), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_log_buf_mr_meta_.rkey), remote_register_info + offset, 4);
    offset += 4;

    printf("从第 %ld 个备节点接收日志缓冲区 %ld 的qp mr信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \nmr_addr: %ld \nmr_size: %ld \nlkey: %ld \nrkey: %ld \n", 
        backup_id_, log_buf_id_, 
        remote_qp_meta_.qp_num, remote_qp_meta_.port_lid, remote_qp_meta_.port_num, 
        remote_log_buf_mr_meta_.mr_addr, remote_log_buf_mr_meta_.mr_size, remote_log_buf_mr_meta_.lkey, remote_log_buf_mr_meta_.rkey);

    /* 在日志复制qp中，设置远端qp_meta的信息 */
    log_replica_qp_->SetRmoteQPMetaData(remote_qp_meta_);
    //初始化qp状态,使其能够对外进行服务
    log_replica_qp_->MofifyQPToReady();
    
    /* 预先放置receive wr */
    for (int slot_idx = 0; slot_idx < recv_mr_meta_.slot_cnt_; slot_idx++)
    {
        AddRecvMessageWR();
    }

}


void LogReplicaComm::ReplicateLog(MemPtr send_addr, MemPtr recv_addr, uint32_t size)
{
    LogReplicaWR* log_replica_wr = new LogReplicaWR();
    log_replica_wr->wr_id_   = 0;
    log_replica_wr->wr_type_ = (uint64_t)LogReplicaWRType::LOG_REPLICA_WR;
    log_replica_wr->replica_log_size_ = size;

    send_wr_queue_.push(log_replica_wr);

    log_replica_qp_->PostWriteWithIMM(send_addr, recv_addr, size, 
                                      local_log_buf_mr_meta_.lkey, remote_log_buf_mr_meta_.rkey, size);
    
    outstanding_wr_++;
}


void LogReplicaComm::AddRecvMessageWR()
{
    if (recv_mr_meta_.is_full_)
        return;
    
    log_replica_qp_->PostRecvWR(recv_mr_meta_.message_pool_ + recv_mr_meta_.first_free_slot_ * recv_mr_meta_.slot_size_, 
                                recv_mr_meta_.slot_size_, recv_mr_meta_.lkey);
    
    RecvMessageWR* recv_message_wr = new RecvMessageWR();
    recv_message_wr->wr_id_   = 0;
    recv_message_wr->wr_type_ = (uint64_t)LogReplicaWRType::RECV_MESSAGE_WR;
    recv_message_wr->slot_idx = recv_mr_meta_.first_free_slot_;
    recv_wr_queue_.push(recv_message_wr);

    recv_mr_meta_.first_free_slot_ = (recv_mr_meta_.first_free_slot_ + 1) % recv_mr_meta_.slot_cnt_;

    if (recv_mr_meta_.first_free_slot_ == recv_mr_meta_.first_used_slot_)
        recv_mr_meta_.is_full_ = true;
}


void LogReplicaComm::ProcessSendWC()
{
    struct ibv_wc wc;
    WorkRequest*  wr = nullptr;

    int ret = 0;
    while (true)
    {
        ret = log_replica_qp_->PullWCFromSendCQ(&wc);
        
        if (ret == 0)
            break;
        else if (ret == 1)
        {
            if( wc.status != IBV_WC_SUCCESS )
                printf("LogReplicaComm::ProcessSendWC获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
            else
            {
                outstanding_wr_--;

                //正常情况下，send_wr_queue_一定不为空
                wr = send_wr_queue_.front();
                send_wr_queue_.pop();

                //对wr进行解析
                switch ((LogReplicaWRType)wr->wr_type_)
                {
                case LogReplicaWRType::LOG_REPLICA_WR :
                    {
                        LogReplicaWR* log_replica_wr = dynamic_cast<LogReplicaWR*>(wr);
                        replicated_lsn_ += log_replica_wr->replica_log_size_;
                        break;
                    }
                default:
                    break;
                }

                delete wr;
                wr = nullptr;
            }


        }
        else
        {
            printf("日志复制过程中出现错误!\n");
        }
    };

}


void LogReplicaComm::ProcessRecvWC()
{
    struct ibv_wc wc;
    WorkRequest*  wr = nullptr;

    int ret = 0;
    while (true)
    {
        ret = log_replica_qp_->PullWCFromRecvCQ(&wc);
        
        if (ret == 0)
            break;
        else if (ret == 1)
        {
            if( wc.status != IBV_WC_SUCCESS )
                printf("LogReplicaComm::ProcessRecvWC 获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
            else
            {
                //正常情况下，recv_wr_queue_一定不为空
                wr = recv_wr_queue_.front();
                recv_wr_queue_.pop();
                
                //对wr进行解析
                switch ((LogReplicaWRType)wr->wr_type_)
                {
                case LogReplicaWRType::RECV_MESSAGE_WR :
                    {
                        uint16_t first_used_slot = recv_mr_meta_.first_used_slot_;
                        ParseAndProcessMessage(first_used_slot);
                        recv_mr_meta_.first_used_slot_ = (first_used_slot + 1) % recv_mr_meta_.slot_cnt_;
                        recv_mr_meta_.is_full_ = false;

                        AddRecvMessageWR();

                        break;
                    }
                default:
                    break;
                }

                delete wr;
                wr = nullptr;
            }
        }
        else
        {
            printf("send/receive消息传递过程中出现错误!\n");
        }
    }
}


void LogReplicaComm::ParseAndProcessMessage(uint16_t slot_num)
{
    MemPtr  start_ptr = recv_mr_meta_.message_pool_ + slot_num * recv_mr_meta_.slot_size_;
    int     offset = 0;
    uint8_t message_type = 0;
    message_type = *reinterpret_cast<uint8_t*>(start_ptr + offset);
    offset += sizeof(message_type);

    switch ((LogReplicaMessageType)message_type)
    {
    case SYN_RECLAIMED_LSN_MESSAGE:
        reclaimed_lsn_ = *(LogLSN*)(start_ptr + offset);

        // if (reclaimed_lsn_ >= 15000000000)
        // {
        //     printf("备节点: %ld, 日志缓冲区: %ld, 已回收日志lsn: %ld\n", backup_id_, log_buf_id_, reclaimed_lsn_);
        // }
        
        // printf("备节点: %ld, 日志缓冲区: %ld, 已回收日志lsn: %ld\n", backup_id_, log_buf_id_, reclaimed_lsn_);
        break;

    default:
        break;
    }
}

