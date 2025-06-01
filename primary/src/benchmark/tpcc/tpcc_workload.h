#ifndef BENCHMARK_TPCC_WORKLOAD_H_
#define BENCHMARK_TPCC_WORKLOAD_H_


#include "config.h"
#include "workload.h"
#include "tpcc_config.h"


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

    // Transaction*  GetNextTxn(uint64_t  thread_id);
    void InsertNewTxn(ThreadID thread_id, Transaction* new_txn);
    
    /*
     * 用于Delivery事务
     *  
     * deliverying控制同一时刻，每个warehouse只能执行一个delivery事务，
     * 换句话说，单个warehouse串行执行delivery事务，两个delivery事务间不存在冲突
     * 
     * undelivered_order，记录每个warehouse下每个district下还未完成delivery
     * 的最小order，初始为0。
     */
    bool      deliverying[g_warehouse_num];
    uint64_t  undelivered_order[g_warehouse_num][10];

};


class NewOrder : public Transaction
{
private:

    //input for neworder
    uint64_t  w_id_;
    uint64_t  d_id_;
    uint64_t  c_id_;
    uint64_t  o_entry_d_;
    uint64_t  ol_cnt_;
    uint64_t* ol_i_ids_;
    uint64_t* ol_supply_w_ids_;
    uint64_t* ol_quantities_;

    /*
     * 事务是否访问了其他warehouse的数据，或者说事务是否为分布式事务
     * 0 表示本地事务, 1 表示分布式事务
     */
    uint64_t remote_;   

    //是否被系统设置为主动回滚，模拟因业务逻辑导致回滚的场景
    //目前未开启
    uint64_t rbk_;      
    


public:
    NewOrder();
    NewOrder(TPCCTxnType txn_type);
    NewOrder(ThreadID thread_id, uint64_t local_txn_id);
    ~NewOrder();

    RC RunTxn();

    //generate input data for neworder
    void GenInputData(uint64_t  ware_id);
    void GenInputData();

};


class Payment : public Transaction
{
private:
    
    uint64_t  w_id_;
    uint64_t  d_id_;
    uint64_t  c_id_;
    uint64_t  c_w_id_;
    uint64_t  c_d_id_;
	char      c_last_[LASTNAME_LEN];
    uint64_t  h_date_;
	uint64_t  h_amount_;

	bool     by_last_name_;


public:
    Payment();
    Payment(TPCCTxnType txn_type);
    ~Payment();

    RC RunTxn();

    //generate input data for neworder
    void GenInputData(uint64_t  ware_id);
    void GenInputData();


};



class Delivery : public Transaction
{
private:
    
    uint64_t w_id_;
    uint64_t o_carrier_id_;
    uint64_t ol_delivery_d_;

public:
    Delivery();
    Delivery(TPCCTxnType txn_type);
    ~Delivery();

    void GenInputData(uint64_t  ware_id);
    void GenInputData();

    RC RunTxn();

};



#endif
#endif