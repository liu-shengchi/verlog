#ifndef  EXECUTOR_WORKLOAD_THREAD_H_
#define  EXECUTOR_WORKLOAD_THREAD_H_

#include "config.h"

#include "db_thread.h"


class Client;


class WorkloadThread : public DBThread 
{
private:

    ThreadID work_thread_id_;


    Client* clients_[g_client_total_num];


public:
    WorkloadThread(ThreadID thread_id, ThreadID work_thread_id, ProcID proc_id);
    ~WorkloadThread();

    void Run();

};



#endif