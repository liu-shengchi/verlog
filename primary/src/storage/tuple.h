#ifndef STORAGE_TUPLE_H_
#define STORAGE_TUPLE_H_

#include "config.h"

class S2plWDTupleMeta;
class SiloTupleMeta;
class MvtoTupleMeta;

class LogTupleMeta;
class SerialTupleMeta;
class TaurusTupleMeta;
class LogIndexTupleMeta;


class Tuple
{
private:

public:

    Tuple();
    Tuple(uint64_t  tuple_size);
    ~Tuple();
    
    TupleData GetTupleData() { return tuple_data_; }

    /* 
     * 将source_tuple的元组数据复制到当前元组中
     */
    void CopyTupleData(Tuple* source_tuple, uint64_t size);
    
    /* 
     * 将source_tuple的元组元数据复制到当前元组的元数据中
     * 当前元组的元数据包括：
     * 1. cc_tuple_meta_  并发控制元数据
     * 2. log_tuple_meta_ 日志机制元数据
     */
    void CopyTupleMeta(Tuple* source_tuple);

    void CopyLogTupleMeta(LogTupleMeta* log_tuple_meta);

    /* 
     * 并发控制相关元数据
     */
#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    S2plWDTupleMeta* cc_tuple_meta_;
#elif CC_STRATEGY_TYPE == SILO_CC
    SiloTupleMeta*   cc_tuple_meta_;
#elif CC_STRATEGY_TYPE == MVTO_CC
    MvtoTupleMeta*   cc_tuple_meta_;
#endif

    /* 
     * 事务日志相关元数据
     */
#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    SerialTupleMeta* log_tuple_meta_;
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    TaurusTupleMeta* log_tuple_meta_;
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    LogIndexTupleMeta* log_tuple_meta_;
#endif

    /** tuple data **/
    TupleData    tuple_data_;

};



#endif