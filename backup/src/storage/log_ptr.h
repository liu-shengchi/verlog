#ifndef STORAGE_LOG_PTR_H_
#define STORAGE_LOG_PTR_H_

#include "config.h"


class LogPtr
{
private:
    
public:
    LogPtr();
    ~LogPtr();
    
    Timestamp visible_ts_;

    //表示LogPtr指向的日志的首地址
    char*     log_addr_;
    
    /* 
     * 日志索引版本链，指向同一个元组的下一个版本
     * 注意：只有在使用版本链作为版本管理策略时，才会存在这个数据域
     */
#if AM_STRATEGY == LOG_INDEX_VERSION_CHAIN_AM
    LogPtr* next_log_ptr_;
#endif

};




#endif