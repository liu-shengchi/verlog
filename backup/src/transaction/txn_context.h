#ifndef   TRANSACTION_TXN_CONTEXT_H_
#define   TRANSACTION_TXN_CONTEXT_H_

#include "config.h"


class TxnThread;
class Client;

class Transaction;
class TxnIdentifier;

class AccessObj;

class AMStrategy;
class TupleVerChainAM;
class LogIndexVerChainAM;
class QueryFreshAM;
class ApplyWritesOnDemandAM;


class TxnContext
{
private:

    /*** 当前执行该事务的事务线程 ***/
    TxnThread*     txn_thread_;

    Client*        client_;


    /*** 当前正在执行的事务 ***/
    TxnIdentifier* txn_id_;
    Transaction*   cur_txn_;

    /*** 事务的只读一致时间戳 ***/
    Timestamp      read_ts_;


    /*** 访问方法 ***/
#if   AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
    TupleVerChainAM*       am_strategy_;
#elif AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    LogIndexVerChainAM*    am_strategy_;
#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    QueryFreshAM*          am_strategy_;
#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    ApplyWritesOnDemandAM* am_strategy_;
#endif

    /* 
     * 
     */
    alignas(CACHE_LINE_SIZE) volatile bool   accessing_logbuf_[g_log_buffer_num];
    alignas(CACHE_LINE_SIZE) volatile LogLSN accessing_lsn_[g_log_buffer_num];


#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

    /* 
     * 采用两个变量标识事务执行一个读请求时的状态
     * is_applying_：在执行该读请求时，是否进入了apply write的阶段。
     * apply_finish_：如果进入了apply write阶段，apply write是否结束。
     * 
     * 1. 执行线程调用apply write之前，需要首先设置: 
     *    is_applying_  = true
     *    apply_finish_ = false
     * 2. 应用线程apply write结束后，设置：
     *    apply_finish_ = true
     * 3. 执行线程判断二者均为true，则执行数据的访问，结束后设置：
     *    is_applying_  = false
     *    apply_finish_ = false
     */
    bool is_applying_;
    bool apply_finish_;
#endif

    
    TXN_STATE    txn_state_;


    //记录获取获取读时间戳的延迟
    ClockTime fetch_read_ts_start_;
    ClockTime fetch_read_ts_end_;

    //记录等待强一致读快照的延迟
    ClockTime wait_scr_snapshot_start_;
    ClockTime wait_scr_snapshot_end_;

    //记录执行事务的延迟
    ClockTime exec_txn_start_;
    ClockTime exec_txn_end_;

    // 是否对time breakdown分析进行抽样
    uint64_t  time_breakdown_frequency_count_;
    bool      is_time_breakdown_sample_;

    // 是否对 micro 分析进行抽样
    uint64_t  micro_frequency_count_;
    bool      is_micro_sample_;


    friend class TxnThread;
    friend class LogIndexVerChainAM;
    friend class TupleVerChainAM;
    friend class QueryFreshAM;
    friend class ApplyWritesOnDemandAM;
    friend class LogBuffer;
    friend class Client;

public:
    TxnContext();
    ~TxnContext();

    /*
     * 设置事务上下文，
     * 目前需要设置访问策略相关参数：
     * @param am_strategy: 事务获取元组数据采用的访问方法
     */
    // void InitContext(TxnThread* txn_thread, AMStrategy* am_strategy);
    
    void InitContext(Client* client, AMStrategy* am_strategy);

    void SetTxnThread(TxnThread* txn_thread);


    
    /* 将事务上下文中的current_txn设置为传入的事务，
     * 并将事务上下文传入事务中，用于执行事务逻辑时调用相关接口，做并发控制 */
    // void InitTxn(Transaction* txn, Timestamp read_ts);

    void InitTxn(Transaction* txn);
    

    Timestamp GetReadTS() {return read_ts_;};

    /* 
     * 
     */
    RC   AccessTuple(AccessObj* access_obj, const TableID table_id, ShardID shard_id, PrimaryKey primary_key, TupleData tuple_data);
    



    /*** 开始执行事务 ***/
    RC   RunTxn();


    /* 
     * 执行事务
     *  
     */
    RC   ExecuteTxn();


    /* 
     * 结束事务
     * 注意，传入的参数rc，仅表明事务执行阶段是提交还是回滚。 
     * FinishTxn还需要执行验证阶段（乐观并发控制）、日志持久化、
     * 事务提交/回滚等动作。
     */
    RC   FinishTxn(RC rc);

    /* 
     * 将txn context重置，以便下一个事务使用
     * 
     */
    void ResetContext();


    ThreadID GetExecuteThreadID();


    bool     IsTimeBreakdownSample();

    bool IsMicroStatisticSample();
    

#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    void StartApplying();
    void EndApplying();
    bool isApplying();

    void WaitApplyWrite();
    void FinishApplyWrite();
    bool isApplyFinish();
#endif

};



#endif