#include "tuple.h"


#include <malloc.h>
#include <string.h>

using namespace std;



/********* Tuple **********/

Tuple::Tuple()
{
    next_tuple_ver_ = nullptr;
    tuple_data_     = nullptr;

#if  AM_STRATEGY_TYPE  == LOG_INDEX_VERSION_CHAIN_AM
    min_ts_ = 0;
    max_ts_ = 0;
#else
    //可见时间戳，对只读时间戳 >= visible_ts_的事务可见
    visible_ts_ = 0;
#endif
}


Tuple::Tuple(uint64_t tuple_size, Timestamp visible_ts)
{   
    next_tuple_ver_ = nullptr;
    tuple_data_     = (TupleData)malloc(tuple_size * sizeof(char));
    memset(tuple_data_, '0', tuple_size);
    
#if  AM_STRATEGY_TYPE  == LOG_INDEX_VERSION_CHAIN_AM
    min_ts_ = visible_ts;
    max_ts_ = MAX_TIMESTAMP;
#else
    //可见时间戳，对只读时间戳 >= visible_ts_的事务可见
    visible_ts_ = visible_ts;
#endif
}


Tuple::Tuple(uint64_t  tuple_size, Timestamp min_ts, Timestamp max_ts)
{   
    next_tuple_ver_ = nullptr;
    tuple_data_     = (TupleData)malloc(tuple_size * sizeof(char));
    memset(tuple_data_, '0', tuple_size);
    
#if  AM_STRATEGY_TYPE  == LOG_INDEX_VERSION_CHAIN_AM
    min_ts_ = min_ts;
    max_ts_ = max_ts;
#else
    //可见时间戳，对只读时间戳 >= visible_ts_的事务可见
    visible_ts_ = min_ts;
#endif
}



Tuple::~Tuple()
{
    free(tuple_data_);
}


void Tuple::CopyTupleData(Tuple* source_tuple, uint64_t size)
{
    memcpy(tuple_data_, source_tuple->tuple_data_, size);
}

void Tuple::CopyTupleData(TupleData source_tuple_data, uint64_t size)
{
    memcpy(tuple_data_, source_tuple_data, size);
}
