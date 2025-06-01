#ifndef COMMUNICATION_RDMA_QP_H_
#define COMMUNICATION_RDMA_QP_H_

#include "config.h"

#include "rdma_meta.h"


#include "infiniband/verbs.h"



class RdmaQP
{
private:

    bool ModifyQPtoInit();
    bool ModifyQPtoRTR();
    bool ModifyQPtoRTS();


    /* RDMA设备、端口、以及QP所在pd */
    struct ibv_context*   device_ctx_; /* RDMA设备上下文 */
    uint32_t              rdma_port_;  /* rdma端口号，通常为1 */
    struct ibv_port_attr* port_attr_;  /* rdma端口属性 */

    struct ibv_pd*    pd_;             /* Protection Domain handler */


    /* qp相关数据结构 */
    struct ibv_qp*  qp_;
    struct ibv_cq*  send_cq_; /* SQ -> CQ */
    struct ibv_cq*  recv_cq_; /* RQ -> CQ */

    uint32_t qp_max_depth_;     /* qp中每个SQ RQ中最大的WR数量 */

    
    /* 与远端相连QP的属性，用于初始化QP工作状态 */
    QPMeta   remote_qp_meta_;


    struct ibv_mr* read_write_mr_;

    struct ibv_mr* send_mr_;
    struct ibv_mr* recv_mr_;



public:
    RdmaQP(struct ibv_mr* read_write_mr, struct ibv_mr* send_mr, struct ibv_mr* recv_mr,
           struct ibv_context* device_ctx, uint32_t rdma_port, struct ibv_port_attr* port_attr, struct ibv_pd* pd);
    ~RdmaQP();

    void GetLocalQPMetaData(QPMeta &local_qp_meta_data);
    void SetRmoteQPMetaData(QPMeta &remote_qp_meta_data);
    
    /* 
     * 将QP的状态调整为可以执行Send/Receive任务
     */
    void MofifyQPToReady();

    /* 
     * rdma write with imm
     * @param: local_addr & lkey  将要传输的内存区域起始地址和传输的大小，注意内存区间 [local_addr, local_addr + length) 必须在MR中
     * @param: remote_addr & rkey  rdma write写入的远端地址，[remote_addr, remote_addr + length)需要处于和本地QP相连的远端QP同一pd下的MR区域中
     * @param: imm_data  传输携带的立即数，用于触发接收端recv wr，提示接收端数据发生变化。imm_data本身可具有实际意义，
     *                   比如可表示传输数据的大小、所在位置等，对imm_data进行精心设计，可极大提高系统通信和同步效率。
     */
    void PostWriteWithIMM(uint64_t  local_addr, uint64_t remote_addr, uint32_t length, uint32_t lkey, uint32_t rkey, uint32_t imm_data);


    void PostReadWR(uint64_t  local_addr, uint64_t remote_addr, uint32_t length, uint32_t lkey, uint32_t rkey, bool send_signaled = true);


    /* 
     * rdma send
     */
    void PostSendWR(uint64_t local_addr, uint32_t length, uint32_t lkey);

    /* 
     * 
     */
    void PostRecvWR(uint64_t local_addr, uint32_t length, uint32_t lkey);

    
    void PostRecvIMMWR(uint64_t local_addr, uint32_t lkey);

    
    /* 
     * 
     */
    int PullWCFromSendCQ(struct ibv_wc* send_wc);

    /* 
     * 
     */
    int PullWCFromRecvCQ(struct ibv_wc* recv_wc);

};







#endif