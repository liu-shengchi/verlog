#include "rdma_qp.h"



RdmaQP::RdmaQP(struct ibv_mr* read_write_mr, struct ibv_mr* send_mr, struct ibv_mr* recv_mr, 
               struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, struct ibv_pd* pd)
{
    device_ctx_ = device_ctx;
    rdma_port_  = rdma_port;
    port_attr_  = port_attr;

    pd_ = pd;

    qp_max_depth_ = g_qp_max_depth;

    //创建send receive queue的完成队列（completion queue）
    send_cq_ = ibv_create_cq(device_ctx_, qp_max_depth_, nullptr, nullptr, 0);
    if (send_cq_ == nullptr) {
        printf("创建send cq失败! \n");
    }

    recv_cq_ = ibv_create_cq(device_ctx_, qp_max_depth_, nullptr, nullptr, 0);
    if (recv_cq_ == nullptr) {
        printf("创建recv cq失败! \n");
    }
    
    /* 创建qp */
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_type    = IBV_QPT_RC;   /* 通信类型为Reliable Connection */
    attr.sq_sig_all = 0;
    attr.send_cq    = send_cq_;
    attr.recv_cq    = recv_cq_;
    attr.cap.max_send_wr     = qp_max_depth_;
    attr.cap.max_recv_wr     = qp_max_depth_;
    attr.cap.max_send_sge    = 1;
    attr.cap.max_recv_sge    = 1;
    attr.cap.max_inline_data = 0;
    
    qp_ = ibv_create_qp(pd_, &attr);

    if (qp_ == nullptr)
    {
        printf("创建QP失败!\n");
    }


    read_write_mr_ = read_write_mr;
    send_mr_       = send_mr;
    recv_mr_       = recv_mr;


    /* 创建消息接收区域 */
    // slot_cnt_  = qp_max_depth_ * 2;
    // slot_size_ = g_max_message_size;
    // message_pool_size_ = slot_cnt_ * slot_size_;
    
    // message_pool_ = (void*)malloc(message_pool_size_);

    // //set mr access rights
    // unsigned int mr_access_type = 0;
    // mr_access_type = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    
    // message_mr_ = ibv_reg_mr(pd_, message_pool_, message_pool_size_, mr_access_type);

    // first_free_slot_ = 0;
    // first_used_slot_ = 0;
    // is_full_         = false;

}

RdmaQP::~RdmaQP()
{

}


bool RdmaQP::ModifyQPtoInit()
{
    struct ibv_qp_attr attr;
    uint64_t flags = 0;
    uint64_t rc;
    memset(&attr, 0, sizeof(attr));
    
    // attr.qp_state   = IBV_QPS_INIT;
    // attr.port_num   = rdma_port_;
    // attr.pkey_index = 0;
    // attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    // flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY | IBV_QP_ACCESS_FLAGS;
    // rc    = ibv_modify_qp(qp_, &attr, flags);
    
    attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = 0;
    attr.port_num        = rdma_port_;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    rc = ibv_modify_qp(qp_, &attr, IBV_QP_STATE |
                                    IBV_QP_PKEY_INDEX |
                                    IBV_QP_PORT       |
                                    IBV_QP_ACCESS_FLAGS);

    if (rc) {
        printf("ModifyQPtoInit失败! error:%ld\n", rc);
        return false;
    }
    else
        return true;    
}


bool RdmaQP::ModifyQPtoRTR()
{
    struct ibv_qp_attr attr;
    uint64_t flags = 0;
    uint64_t rc;
    memset(&attr, 0, sizeof(attr));

    // attr.qp_state = IBV_QPS_RTR;
    // attr.path_mtu = IBV_MTU_4096;
    // attr.dest_qp_num = remote_qp_meta_.qp_num;
    // attr.rq_psn = 3185;

    // attr.ah_attr.is_global = 0;
    // attr.ah_attr.dlid = remote_qp_meta_.port_lid;
    // attr.ah_attr.sl   = 0;
    // attr.ah_attr.src_path_bits = 0;
    // attr.ah_attr.port_num = remote_qp_meta_.port_num;

    // flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;

    // attr.max_dest_rd_atomic = 16;
    // attr.min_rnr_timer = 12;
    // flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    // rc = ibv_modify_qp(qp_, &attr, flags);

    attr.qp_state		= IBV_QPS_RTR;
    attr.path_mtu		= IBV_MTU_4096;
    attr.dest_qp_num	= remote_qp_meta_.qp_num;
    attr.rq_psn		    = 3185;

    attr.max_dest_rd_atomic	= 1;
    attr.min_rnr_timer	    = 12;
    attr.ah_attr.is_global	= 0;
    attr.ah_attr.dlid	    = remote_qp_meta_.port_lid;
    attr.ah_attr.sl		    = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num	   = remote_qp_meta_.port_num;
    
    rc = ibv_modify_qp(qp_, &attr, IBV_QP_STATE               |
                                    IBV_QP_AV                 |
                                    IBV_QP_PATH_MTU           |
                                    IBV_QP_DEST_QPN           |
                                    IBV_QP_RQ_PSN             |
                                    IBV_QP_MAX_DEST_RD_ATOMIC |
                                    IBV_QP_MIN_RNR_TIMER);

    if (rc) {
        printf("ModifyQPtoRTR失败! error:%ld\n", rc);
        return false;
    }
    else
        return true;   
}


bool RdmaQP::ModifyQPtoRTS()
{
    struct ibv_qp_attr attr;
    uint64_t flags = 0;
    uint64_t rc;
    memset(&attr, 0, sizeof(attr));

    // attr.qp_state = IBV_QPS_RTS;
    // attr.sq_psn = 3200;
    // flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    // attr.timeout   = 14;
    // attr.retry_cnt = 7;
    // attr.rnr_retry = 7;
    // attr.max_rd_atomic      = 16;
    // attr.max_dest_rd_atomic = 16;
    // flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

    // rc = ibv_modify_qp(qp_, &attr, flags);

    attr.qp_state	    = IBV_QPS_RTS;
    attr.sq_psn	        = 3200;
    attr.timeout	    = 14;
    attr.retry_cnt	    = 7;
    attr.rnr_retry	    = 7; /* infinite */
    attr.max_rd_atomic  = 1;
    
    rc = ibv_modify_qp(qp_, &attr, IBV_QP_STATE              |
                                   IBV_QP_TIMEOUT            |
                                   IBV_QP_RETRY_CNT          |
                                   IBV_QP_RNR_RETRY          |
                                   IBV_QP_SQ_PSN             |
                                   IBV_QP_MAX_QP_RD_ATOMIC);

    if (rc) {
        printf("ModifyQPtoRTS失败! error:%ld\n", rc);
        return false;
    }
    else
        return true;   
}



void RdmaQP::MofifyQPToReady()
{
    /* 改变qp的状态，准备进行send/receive操作 */
    // if (!ModifyQPtoInit())
    //     printf("ModifyQPtoInit失败!\n");
    // if (!ModifyQPtoRTR())
    //     printf("ModifyQPtoRTR失败!\n");
    // if (ModifyQPtoRTS())
    //     printf("ModifyQPtoRTS失败!\n");

    ModifyQPtoInit();
    ModifyQPtoRTR();
    ModifyQPtoRTS();

}


void RdmaQP::PostWriteWithIMM(uint64_t  local_addr, uint64_t remote_addr, uint32_t length, uint32_t lkey, uint32_t rkey, uint32_t imm_data)
{
    struct ibv_sge      sg;
    struct ibv_send_wr  wr;
    struct ibv_send_wr *wrBad;

    //初始化发送的地址、大小以及lkey
    memset(&sg, 0, sizeof(sg));
    sg.addr   = local_addr;
    sg.length = length;
    sg.lkey   = lkey;

    //初始化RDMA发送信息以及远端访问信息
    memset(&wr, 0, sizeof(wr));
    wr.wr_id    = 0;
    wr.sg_list  = &sg;
    wr.num_sge  = 1;
    wr.opcode   = IBV_WR_RDMA_WRITE_WITH_IMM;   //采用RDMA单边写操作，需要指明远端写入的地址和rkey
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey        = rkey;

    wr.send_flags = IBV_SEND_SIGNALED;    
    wr.imm_data   = imm_data;

    ibv_post_send(qp_, &wr, &wrBad);
}


void RdmaQP::PostReadWR(uint64_t  local_addr, uint64_t remote_addr, uint32_t length, uint32_t lkey, uint32_t rkey, bool send_signaled)
{
    struct ibv_sge      sg;
    struct ibv_send_wr  wr;
    struct ibv_send_wr *bad_wr;
    
    memset(&sg, 0, sizeof(sg));
    sg.addr   = local_addr;
    sg.length = length;
    sg.lkey   = lkey;
    
    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = 0;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_RDMA_READ;
    
    if (send_signaled)
        wr.send_flags = IBV_SEND_SIGNALED;

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey        = rkey;
    
    if (ibv_post_send(qp_, &wr, &bad_wr)) {
        fprintf(stderr, "Error, ibv_post_send() failed\n");
    }
}


void RdmaQP::PostSendWR(uint64_t local_addr, uint32_t length, uint32_t lkey)
{
    struct ibv_sge      sg;
    struct ibv_send_wr  wr;
    struct ibv_send_wr *wrBad;

    //初始化发送的地址、大小以及lkey
    memset(&sg, 0, sizeof(sg));
    sg.addr   = local_addr;
    sg.length = length;
    sg.lkey   = lkey;

    //初始化RDMA发送信息以及远端访问信息
    memset(&wr, 0, sizeof(wr));
    wr.wr_id    = 0;
    wr.sg_list  = &sg;
    wr.num_sge  = 1;
    wr.opcode   = IBV_WR_SEND;      //采用RDMA双边send操作，需要指明发送的本地地址和大小
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(qp_, &wr, &wrBad) != 0) {
        printf("RdmaQueuePair::PostSendWR() failed!\n");
    }
}


void RdmaQP::PostRecvWR(uint64_t local_addr, uint32_t length, uint32_t lkey)
{
    struct ibv_sge      sg;
    struct ibv_recv_wr  wr;
    struct ibv_recv_wr *wrBad;

    //初始化发送的地址、大小以及lkey
    memset(&sg, 0, sizeof(sg));
    sg.addr   = local_addr;
    sg.length = length;
    sg.lkey   = lkey;

    //初始化RDMA发送信息以及远端访问信息
    memset(&wr, 0, sizeof(wr));
    wr.wr_id    = 0;
    wr.sg_list  = &sg;
    wr.num_sge  = 1;

    if (ibv_post_recv(qp_, &wr, &wrBad) != 0) {
        printf("RdmaQueuePair::PostReceive() failed!\n");
    }
}


void RdmaQP::PostRecvIMMWR(uint64_t local_addr, uint32_t lkey)
{
    struct ibv_sge      sg;
    struct ibv_recv_wr  wr;
    struct ibv_recv_wr *wrBad;

    memset(&sg, 0, sizeof(sg));
    sg.addr   = local_addr;
    sg.length = 0;   // because of IBV_WR_RDMA_WRITE_WITH_IMM,
                     // we don't need to worry about the recv buffer
    sg.lkey   = lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = 0;
    wr.sg_list = &sg;
    wr.num_sge = 1;

    if (ibv_post_recv(qp_, &wr, &wrBad) != 0) {
        printf("RdmaQueuePair::PostRecvIMMWR() failed!\n");
    }
}


int RdmaQP::PullWCFromSendCQ(struct ibv_wc* send_wc)
{
    return ibv_poll_cq(send_cq_, 1, send_wc);
}

int RdmaQP::PullWCFromRecvCQ(struct ibv_wc* recv_wc)
{
    return ibv_poll_cq(recv_cq_, 1, recv_wc);
}


void RdmaQP::GetLocalQPMetaData(QPMeta &local_qp_meta_data)
{
    local_qp_meta_data.qp_num   = qp_->qp_num;
    local_qp_meta_data.port_lid = port_attr_->lid;
    local_qp_meta_data.port_num = rdma_port_;
}

void RdmaQP::SetRmoteQPMetaData(QPMeta &remote_qp_meta_data)
{
    remote_qp_meta_.qp_num   = remote_qp_meta_data.qp_num;
    remote_qp_meta_.port_lid = remote_qp_meta_data.port_lid;
    remote_qp_meta_.port_num = remote_qp_meta_data.port_num;
}
