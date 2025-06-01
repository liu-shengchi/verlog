#ifndef STORAGE_TUPLE_H_
#define STORAGE_TUPLE_H_

#include "config.h"

class Tuple
{
private:

public:

    Tuple();
    Tuple(uint64_t tuple_size, Timestamp visible_ts);
    Tuple(uint64_t tuple_size, Timestamp min_ts, Timestamp max_ts);
    ~Tuple();
    
    TupleData GetTupleData() { return tuple_data_; }

    /* 
     * 将source_tuple的元组数据复制到当前元组中
     */
    void CopyTupleData(Tuple* source_tuple, uint64_t size);

    void CopyTupleData(TupleData source_tuple_data, uint64_t size);

    /* 
     * 用于构造版本链
     * 在当前的设计中，TupleVerChain模式采用 N2O 方式维护版本链，
     * 在LogIndexVerChain模式中，由于垃圾回收和long-running事务，会按需创建
     * 支持long-running事务一致的版本链，方向是 O2N。
     */
    Tuple*       next_tuple_ver_;


#if  AM_STRATEGY_TYPE  == LOG_INDEX_VERSION_CHAIN_AM


    Timestamp   min_ts_;
    Timestamp   max_ts_;
#else
    //可见时间戳，对只读时间戳 >= visible_ts_的事务可见
    Timestamp    visible_ts_;
#endif


    /** tuple data **/
    TupleData    tuple_data_;

};



#endif