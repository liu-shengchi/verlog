#ifndef COMMUNICATION_RDMA_META_H_
#define COMMUNICATION_RDMA_META_H_

#include "config.h"


typedef struct QPMeta
{
    uint32_t qp_num;    /* qp的号 */
    uint16_t port_lid;  /* qp所在端口的LID */
    uint8_t  port_num;  /* qp使用端口号 */
    // uint8_t  gid[16];  /* 只有在跨子网通信时才会用到，目前系统不支持 */
}QPMeta;


/* 
 * 为rdma read/write操作构建内存区域，用于大数据量传输
 * 一个QP包含一个read/write内存区域。
 */
typedef struct RWMRMeta
{
    MemPtr   mr_addr;
    uint64_t mr_size;
    uint32_t lkey;
    uint32_t rkey;
}RWMRMeta;



/* 
* 为Send / Receive 构建消息缓冲区MessagePool，用于消息传递
* 系统使用RDMA Send/Receive实现系统间控制信息的传递
* 目前同一个messagePool只能选择Send/Receive其中一种用途
* 如果一个QP既需要Send也需要Receive，则需要分别创建message_pool，分开存放消息，
* 或者创建不同的QP，分别执行Send / Receive。
*/
typedef struct SRMRMeta
{
    MemPtr            message_pool_;
    uint32_t          message_pool_size_;
    uint16_t          slot_cnt_;      /* 2*QP_MAX_DEPTH */
    uint16_t          slot_size_;     /* 大于等于最大的消息 */

    volatile uint16_t first_free_slot_;
    volatile uint16_t first_used_slot_;
    volatile bool     is_full_;

    uint32_t          lkey;

}SRMRMeta;



class WorkRequest
{
public:
    uint64_t wr_type_;
    uint64_t wr_id_;

    virtual void set() = 0;
};


/* 消息头 */
class MessageMeta
{
public:
    uint8_t message_type;

    virtual void set() = 0;
};


class TestMessage : public MessageMeta
{
public:
    uint64_t key;
    uint64_t value;

    void set(){};
};



#endif