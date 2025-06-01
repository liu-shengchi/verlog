#ifndef BENCHMARK_YCSB_WORKLOAD_H_
#define BENCHMARK_YCSB_WORKLOAD_H_

#include "config.h"
#include "workload.h"

#include "ycsb_config.h"

using namespace std;

class Workload;

#if   WORKLOAD_TYPE == YCSB_W


class YCSBWorkload : public Workload
{
public:
    YCSBWorkload();
    ~YCSBWorkload();

    Transaction* GenTxn();

    void InsertNewTxn(ThreadID thread_id, Transaction* new_txn);
};



enum ReqType{RD,WR};

struct YCSBRequest
{
    ReqType    type;
    ShardID    shard_id;
    PrimaryKey key;
};


class YCSBTransaction : public Transaction
{
private:

public:
    YCSBTransaction(YCSBTxnType txn_type);
    ~YCSBTransaction();

    void GenInputData(uint64_t shard_id);
    void GenInputData();

    RC RunTxn();

    uint64_t     request_cnt_;
    YCSBRequest  requests_[g_ycsb_request_num_per_txn];
};



#endif
#endif