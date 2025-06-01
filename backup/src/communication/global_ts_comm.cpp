#include "global_ts_comm.h"
#include "global.h"

#include "db_comm.h"

#include "rdma_qp.h"

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>


GlobalTSComm::GlobalTSComm(struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, 
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

    //set mr access rights
    unsigned int mr_access_type = 0;
    mr_access_type = IBV_ACCESS_LOCAL_WRITE;
    
    global_ts_mr_ = ibv_reg_mr(pd_, (void*)&g_commit_ts, sizeof(Timestamp), mr_access_type);

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


    fetching_cts_ = false;

    latest_fetched_arrive_ct_ = 0;
    max_fetching_arrive_ct_   = 0;

}

GlobalTSComm::~GlobalTSComm()
{

}


void GlobalTSComm::RegisterWithRemoteQP()
{
    /* QPMeta(4+2+1) */
    uint32_t local_info_size  = 7;
    /* QPMeta(4+2+1) + MRMeta(8+8+4+4) */
    uint32_t remote_info_size = 31;
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
    
    printf("向主节点发送的qp mr信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \n", 
            local_qp_meta_.qp_num, local_qp_meta_.port_lid, local_qp_meta_.port_num);

    //offset == register_info_size

    /* 和远端服务器建立socket连接，交换 qp mr 信息 */
    //远端服务器ip port
    std::string remote_ip   = primary_ip;
    uint32_t    remote_port = primary_port;

    printf("与主节点建立socket连接!\n");

    //建立socket连接
    int recv_sock = 0;
    int recv_fd   = 0;
    recv_sock = CreateSocketListen();
    recv_fd   = AcceptSocketConnect(recv_sock);
    recv(recv_fd, remote_register_info, remote_info_size, 0);

    printf("与主节点交换qp mr信息!\n");

    int send_sock = 0;
    send_sock = ConnectSocket(remote_ip, remote_port);
    send(send_sock, local_register_info, local_info_size, 0);

    close(recv_sock);
    close(recv_fd);
    close(send_sock);
    printf("与主节点关闭socket连接!\n");

    /* 解析远端qp的信息 */
    offset = 0;
    memcpy((char*)&(remote_qp_meta_.qp_num), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_qp_meta_.port_lid), remote_register_info + offset, 2);
    offset += 2;
    memcpy((char*)&(remote_qp_meta_.port_num), remote_register_info + offset, 1);
    offset += 1;
    
    memcpy((char*)&(remote_global_ts_mr_meta_.mr_addr), remote_register_info + offset, 8);
    offset += 8;
    memcpy((char*)&(remote_global_ts_mr_meta_.mr_size), remote_register_info + offset, 8);
    offset += 8;
    memcpy((char*)&(remote_global_ts_mr_meta_.lkey), remote_register_info + offset, 4);
    offset += 4;
    memcpy((char*)&(remote_global_ts_mr_meta_.rkey), remote_register_info + offset, 4);
    offset += 4;

    printf("从主节点接收的qp mr信息: \nqp_num: %ld \nport_lid: %ld \nport_num: %ld \nmr_addr: %ld \nmr_size: %ld \nlkey: %ld \nrkey: %ld \n", 
            remote_qp_meta_.qp_num, remote_qp_meta_.port_lid, remote_qp_meta_.port_num, 
            remote_global_ts_mr_meta_.mr_addr, remote_global_ts_mr_meta_.mr_size, remote_global_ts_mr_meta_.lkey, remote_global_ts_mr_meta_.rkey);

    /* 在本地全局时间戳通信管理中，设置远端qp mr的信息 */
    global_ts_qp_->SetRmoteQPMetaData(remote_qp_meta_);
    //初始化qp状态,使其能够对外进行服务
    global_ts_qp_->MofifyQPToReady();

}


Timestamp GlobalTSComm::FetchGlobalCTS(ClockTime arrive_ct)
{
#if  LLT_OPTIMATION
    if (arrive_ct <= latest_fetched_arrive_ct_)
    {
        // printf("直接返回g_commit_ts!\n");
        ATOM_ADD(count1, 1);
        return g_commit_ts;
    }

    /*
     * 这一步保证 arrive_ct <= max_fetching_arrive_ct_
     */
    ClockTime clock_time = 0;
    while (true)
    {        
        clock_time = max_fetching_arrive_ct_;
        if (arrive_ct > clock_time)
        {
            if (ATOM_CAS(max_fetching_arrive_ct_, clock_time, arrive_ct))
            {
                break;
            }
        }
        else
        {
            break;
        }
    }

    while (true)
    {
        if (ts_comm_mutex_.try_lock())
        {
            //加锁成功
            fetching_cts_ = true;
            /*
             * 修改latest_fetched_arrive_ct_
             * 注意，要保证该条语句在获取时间戳前执行
             */
            latest_fetched_arrive_ct_ = max_fetching_arrive_ct_;
            COMPILER_BARRIER

            //向主节点fetch全局提交时间戳 TODO
            // g_commit_ts += 1;
            Timestamp prev_ts = g_commit_ts;
            global_ts_qp_->PostReadWR(local_global_ts_mr_meta_.mr_addr, 
                                     remote_global_ts_mr_meta_.mr_addr, 
                                     local_global_ts_mr_meta_.mr_size, 
                                     local_global_ts_mr_meta_.lkey, 
                                     remote_global_ts_mr_meta_.rkey,
                                     true);

            struct ibv_wc wc;
            
            int ret = 0;
            while (true)
            {
                ret = global_ts_qp_->PullWCFromSendCQ(&wc);    
                if (ret == 0)
                {
                    // printf("全局时间戳等待获取CQ!\n");
                    continue;
                }
                else if (ret == 1)
                {
                    if( wc.status != IBV_WC_SUCCESS )
                        printf("获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
                    else
                    {
                        if (g_commit_ts < prev_ts)
                        {
                            printf("时间戳获取错误!\n");
                        }
                        // printf("获取到全局时间戳: %" PRIu64 "\n", g_commit_ts);
                    }

                    break;
                }
                else
                    printf("获取全局提交时间戳过程中出现错误!\n");
            }


            COMPILER_BARRIER
            fetching_cts_ = false;

            //放锁，退出循环
            ts_comm_mutex_.unlock();
            ATOM_ADD(count3, 1);
            break;
        }

        if (arrive_ct <= latest_fetched_arrive_ct_)
        {
            //其他线程获取了锁，并修改了latest_fetched_arrive_ct_
            //还在获取最新的全局提交时间戳
            while (fetching_cts_)
                PAUSE
            
            //获取成功
            ATOM_ADD(count2, 1);
            break;
        }
    }

#else
    Timestamp prev_ts = g_commit_ts;
    global_ts_qp_->PostReadWR(local_global_ts_mr_meta_.mr_addr, 
                            remote_global_ts_mr_meta_.mr_addr, 
                            local_global_ts_mr_meta_.mr_size, 
                            local_global_ts_mr_meta_.lkey, 
                            remote_global_ts_mr_meta_.rkey,
                            true);

    struct ibv_wc wc;

    int ret = 0;
    while (true)
    {
        ret = global_ts_qp_->PullWCFromSendCQ(&wc);    
        if (ret == 0)
        {
            // printf("全局时间戳等待获取CQ!\n");
            continue;
        }
        else if (ret == 1)
        {
            if( wc.status != IBV_WC_SUCCESS )
                printf("获取到cq failed状态: %s (%d)\n", ibv_wc_status_str(wc.status), wc.status);
            else
            {
                if (g_commit_ts < prev_ts)
                {
                    printf("时间戳获取错误!\n");
                }
                // printf("获取到全局时间戳: %" PRIu64 "\n", g_commit_ts);
            }   
            break;
        }
        else
            printf("获取全局提交时间戳过程中出现错误!\n");
    }


#endif

    // printf("等待并处理后返回g_commit_ts!\n");
    return g_commit_ts;
}
