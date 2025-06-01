#ifndef  STORAGE_ACCESS_ENTRY_H_
#define  STORAGE_ACCESS_ENTRY_H_

#include "config.h"


class Tuple;


class IndexAndOpt
{
public:

    IndexAndOpt();

    IndexID    index_id_;
    AccessType index_opt_;
};


class AccessEntry
{
public:

    AccessEntry();

    /** 事务访问该元组的操作类型 **/
    AccessType 	access_type_;
    
    /** 元组所在的表 **/
    TableID     table_id_;
    /** 元组所在的分区 **/
    ShardID     shard_id_;
    
    /** 只记录对二级索引产生更新（insert/remove）的索引ID及操作类型 **/
    uint64_t    index_and_opt_cnt_;
    IndexAndOpt index_and_opts_[g_max_sec_indx_per_table];

    uint32_t    access_entry_index_;

    /* 
     * 对于不同的并发控制策略，这两个变量有不同的含义
     * 2PL：
     *     二者均表示访问的元组，2PL允许事务直接对元组进行修改
     * MVCC：
     *     origin表示访问的元组所在的版本链；operate_tuple_表示实际操作的元组版本，如果是读操作
     * 则表示读一致的版本（只读），如果是写操作则为新创建的、复制上一个元组数据的版本（允许写入）
     * OCC：
     *     origin表示访问的全局元组，operate表示本地元组，事务的访问与修改均对operate_tuple_
     * 进行。在事务提交阶段，再将修改同步到origin_tuple_中。
     * 
     * operate_tuple_ 包含了事务修改元组的数据与信息，不仅用于支持并发控制，也用于支持构造事务日志。
     * 因此，每个并发控制策略都需要谨慎处理operate_tuple_，为日志机制提供足够的构造日志信息。
     */
    Tuple*      origin_tuple_;
    Tuple*      operate_tuple_;

    
    /*
     * 事务修改了元组哪些属性
     * 哪一个bit为1，表示对应第几个属性被修改。
     * 因此，当前系统支持单个表最多拥有64个属性
     * 
     * 用于支持细粒度的并发控制和日志生成机制。
     * 
     * TODO: 目前还未使用该属性，待后续添加相关功能
     */
    uint64_t modified_attr_bits_;


    virtual void Reset(){};

};




#endif