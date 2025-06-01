#ifndef   TRANSACTION_TXN_CONTEXT_H_
#define   TRANSACTION_TXN_CONTEXT_H_

#include "config.h"

#include "access_entry.h"

class Transaction;
class TxnIdentifier;

class TxnThread;

class Tuple;
class AccessEntry;

class CCStrategy;
class CCTxnMeta;

class S2plWDStrategy;
class S2plWDTxnMeta;

class SiloStrategy;
class SiloTxnMeta;

class MvtoStrategy;
class MvtoTxnMeta;

class LogStrategy;

class SerialLog;
class SerialTxnMeta;

class TaurusLog;
class TaurusTxnMeta;

class LogIndexLog;
class LogIndexTxnMeta;

class TxnLog;
class LogBuffer;



class TxnContext
{
private:

    /*** 当前执行该事务的事务线程 ***/
    TxnThread*     txn_thread_;

    TxnIdentifier* txn_id_;
    Transaction*   current_txn_;
    

    /******************************************************/
    /************** 并发控制相关事务上下文变量 **************/
    /******************************************************/
    uint32_t        next_access_entry_;
    AccessEntry*    txn_access_entry_[MAX_TUPLE_ACCESS_PER_TXN];

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    S2plWDStrategy* cc_strategy_;
    S2plWDTxnMeta*  cc_txn_meta_;
#elif CC_STRATEGY_TYPE == SILO_CC
    SiloStrategy*   cc_strategy_;
    SiloTxnMeta*    cc_txn_meta_;
#elif CC_STRATEGY_TYPE == MVTO_CC
    MvtoStrategy*   cc_strategy_;
    MvtoTxnMeta*    cc_txn_meta_;
#endif


    /******************************************************/
    /***************** 日志相关事务上下文变量 ***************/
    /******************************************************/
#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    SerialLog*       log_strategy_;
    SerialTxnMeta*   log_txn_meta_;
#elif   LOG_STRATEGY_TYPE == TAURUS_LOG
    TaurusLog*       log_strategy_;
    TaurusTxnMeta*   log_txn_meta_;
#elif   LOG_STRATEGY_TYPE == LOGINDEX_LOG
    LogIndexLog*     log_strategy_;
    LogIndexTxnMeta* log_txn_meta_;
#endif


    /** 用于构造、存放事务本地日志，之后会将日志同步到日志缓冲区 **/
    TxnLog*    txn_log_;
    /** 事务日志的起始LSN和大小 **/
    LogLSN     start_log_lsn_;
    uint64_t   log_size_;

#if DISTRIBUTED_LOG == true
    TxnLog*    remote_txn_log_;
    LogLSN     remote_log_lsn_;
    uint64_t   remote_log_size_;
#endif

    /*
     * 事务持久化日志的日志缓冲区
     * 每个事务会绑定一个日志缓冲区（log_buffer_），将事务日志写入日志缓冲区中。
     * 如果日志策略为分布式日志，比如（Plover、GSN等），则会设置添加变量（remote_log_buffer_）,
     * 表示分布式日志的远程部分，对应的日志缓冲区。
     * 
     * 注意，remote_log_buffer_一般情况下只有在事务开始运行前才确定，因此需要在事务运行前设置。
     */
    LogBufID   log_buf_id_;
    LogBuffer* log_buffer_;
    
#if DISTRIBUTED_LOG == true
    LogBuffer* remote_log_buffer_;
#endif


    /**********************************************/
    /******************* 友元 *********************/
    /**********************************************/
    //并发控制
    friend class S2plWDStrategy;
    friend class SiloStrategy;
    friend class MvtoStrategy;

    //日志机制
    friend class SerialLog;
    friend class TaurusLog;
    friend class LogIndexLog;

public:
    TxnContext();
    ~TxnContext();

    /*
     * 设置事务上下文，
     * 目前需要设置的上下文参数包括并发控制、日志相关的参数：
     * @param cc_strategy: 事务采用的并发控制策略
     * @param log_strategy: 事务采用的日志策略
     * @param log_buffer: 事务将日志写入的日志缓冲区
     * @param remote_log_buffer: 仅用于分布式日志，事务日志分散在两个日志缓冲区中
     */
    void InitContext(TxnThread* txn_thread, CCStrategy* cc_strategy, LogStrategy* log_strategy, LogBuffer* log_buffer, LogBuffer* remote_log_buffer = nullptr);
    

    /* 将事务上下文中的current_txn设置为传入的事务，
     * 并将事务上下文传入事务中，用于执行事务逻辑时调用相关接口，做并发控制 */
    void InitTxn(Transaction* txn);
    

    /* 
     * 并发控制最核心的函数，每次事务访问元组前，都需要调用AccessTuple对元组进行并发控制处理。
     * 传入的参数包含该元组的全部访问信息，可根据这些信息，重新通过索引等访问方法定位该元组。
     * 参数参数：
     * @param access_type:    访问类型，表示事务对该元组的操作是何种类型
     * @param table_id:       元组所在的表
     * @param shard_id:       元组所在的分区
     * @param index_and_opts: 记录下访问该元组对元组进行操作，会对哪些索引进行何种操作
     * @param origin_tuple:   事务根据访问方法（例如索引）等获取到的元组，此时事务不能直接处理该元组
     * @param operate_tuple:  经过并发控制处理后，允许事务访问并处理的元组，根据并发控制策略的不同，
     *                        operate_tuple 和 origin_tuple 有可能指向相同的内存区域（例如 2PL）；
     *                        有可能指向不同的内存区域（例如 OCC，operate_tuple是在事务本地的元组副本）
     */
    RC   AccessTuple(AccessType access_type, TableID table_id, ShardID shard_id, uint64_t index_and_cnt, IndexAndOpt index_and_opts[], Tuple* origin_tuple, Tuple* &operate_tuple);


    /*** 开始执行事务 ***/
    RC   RunTxn();

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
    
};



#endif