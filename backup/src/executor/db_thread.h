#ifndef  EXECUTOR_DB_THREAD_H_
#define  EXECUTOR_DB_THREAD_H_


#include "config.h"


class DBThread
{
protected:

    //线程类型
    DBThreadType thread_type_;
    //线程ID
    ThreadID     thread_id_;
    //线程绑定的处理器id
    uint64_t     processor_id_;
    
    void SetAffinity();

public:

    virtual void Run() = 0;

};





#endif