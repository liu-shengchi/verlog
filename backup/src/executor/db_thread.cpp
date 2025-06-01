#include "db_thread.h"

#include <cstddef>
#include <stdio.h>
#include <errno.h>

void DBThread::SetAffinity()
{
    /*
     * 按照processor_id，将本线程绑定到指定CPU核心
     */
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(processor_id_, &cpu_set);
    
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set);
}
