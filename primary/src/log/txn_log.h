#ifndef TXN_LOG_H_
#define TXN_LOG_H_

#include "config.h"


/* 
 * 每个线程拥有独立的一个TxnLog，充当本地日志缓冲区
 * 用于存放该线程提交事务的日志。
 */
class TxnLog
{
public:
    TxnLog(uint64_t buffer_size);
    ~TxnLog();

    //txn_log_buffer_的大小
    const uint64_t buffer_size_;
    
    //日志偏移量，最小还未使用的buffer所在位置
    uint64_t log_offset_;
    //本地日志缓冲区
    char*    txn_log_buffer_;

};



#endif
