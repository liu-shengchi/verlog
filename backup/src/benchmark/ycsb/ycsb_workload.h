#ifndef BENCHMARK_YCSB_WORKLOAD_H_
#define BENCHMARK_YCSB_WORKLOAD_H_

#include "config.h"
#include "workload.h"


using namespace std;

class Workload;

#if   WORKLOAD_TYPE == YCSB_W


class YCSBWorkload : public Workload
{
public:
    YCSBWorkload();
    ~YCSBWorkload();

    Transaction* GenTxn();
    // Transaction* GenTxn(ThreadID thread_id);

    // bool InsertNewTxn(ThreadID thread_id, Transaction* new_txn);
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

    ShardID      shard_id_;
    PrimaryKey   start_key_;

    uint64_t     scan_cnt_;
};


#endif
#endif