#ifndef BENCHMARK_TPCC_WORKLOAD_H_
#define BENCHMARK_TPCC_WORKLOAD_H_


#include "config.h"
#include "workload.h"

#include <atomic>

using namespace std;

class Workload;


#if   WORKLOAD_TYPE == TPCC_W


class TPCCWorkload : public Workload
{
private:


public:
    TPCCWorkload();
    ~TPCCWorkload();

    
    Transaction* GenTxn();

    // Transaction* GenTxn(ThreadID thread_id);
    // bool InsertNewTxn(ThreadID thread_id, Transaction* new_txn);
};


class OrderStatus : public Transaction
{
private:

    uint64_t w_id_;
    uint64_t d_id_;

    bool     by_last_name_;
    uint64_t c_id_;
    char     c_last_name_[16];


public:
    OrderStatus(TPCCTxnType txn_type);
    ~OrderStatus();

    RC   RunTxn();
    void GenInputData(uint64_t ware_id);
    void GenInputData();

};


class StockLevel : public Transaction
{
private:

    uint64_t w_id_;
    uint64_t d_id_;
    uint64_t stock_min_threshold_;


public:
    StockLevel(TPCCTxnType txn_type);
    ~StockLevel();
    
    RC   RunTxn();
    void GenInputData(uint64_t ware_id);
    void GenInputData();
};


class StockLevelInWH : public Transaction
{
private:

    uint64_t w_id_;
    
    uint64_t start_i_id_;
    uint64_t end_i_id_;

    uint64_t stock_min_threshold_;


public:
    StockLevelInWH(TPCCTxnType txn_type);
    ~StockLevelInWH();
    
    RC   RunTxn();
    void GenInputData(uint64_t ware_id);
    void GenInputData();
};



#endif
#endif