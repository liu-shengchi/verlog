#ifndef BENCHMARK_SMALLBANK_WORKLOAD_H_
#define BENCHMARK_SMALLBANK_WORKLOAD_H_


#include "config.h"
#include "workload.h"

#include "smallbank_config.h"


class Workload;


#if   WORKLOAD_TYPE == SMALLBANK_W


class SmallBankWorkload : public Workload
{
private:


public:
    SmallBankWorkload();
    ~SmallBankWorkload();

    Transaction* GenTxn();

    void InsertNewTxn(ThreadID thread_id, Transaction* new_txn);
};


/*
 * TODO:
 * 1. 分布式事务，一个事务访问两个shard
 */


class Balance : public Transaction
{
private:

    uint64_t a_name_;


public:
    Balance(SMALLBANKTxnType txn_type);
    ~Balance();

    void GenInputData(ShardID shard_id);
    void GenInputData();
    
    RC RunTxn();

};


class DespositChecking : public Transaction
{
private:

    uint64_t a_name_;
    double   c_balance_add_;

public:
    DespositChecking(SMALLBANKTxnType txn_type);
    ~DespositChecking();

    void GenInputData(ShardID shard_id);
    void GenInputData();

    RC RunTxn();

};



class TransactSaving : public Transaction
{
private:
    
    uint64_t a_name_;
    double   s_balance_add_;

public:
    TransactSaving(SMALLBANKTxnType txn_type);
    ~TransactSaving();

    void GenInputData(ShardID shard_id);
    void GenInputData();

    RC RunTxn();

};


class Amalgamate : public Transaction
{
private:
    
    uint64_t a_name_1_;
    uint64_t a_name_2_;


public:
    Amalgamate(SMALLBANKTxnType txn_type);
    ~Amalgamate();

    void GenInputData(ShardID shard_id);
    void GenInputData();

    RC RunTxn();

};


class WriteCheck : public Transaction
{
private:
    
    uint64_t a_name_;
    double   value_;

public:
    WriteCheck(SMALLBANKTxnType txn_type);
    ~WriteCheck();

    void GenInputData(ShardID shard_id);
    void GenInputData();

    RC RunTxn();

};



#endif
#endif