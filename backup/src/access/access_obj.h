#ifndef ACCESS_ACCESS_INDEX_OBJ_H_
#define ACCESS_ACCESS_INDEX_OBJ_H_

#include "config.h"

#include "db_spin_lock.h"


class AccessObj
{    
public:
    

    DBSpinLock  spin_lock_;

    volatile bool deleted_ = false;

#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    PrimaryKey   pk_ = 0;     //use primary_key

#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    PrimaryKey   oid_ = 0;     //use primary_key as oid in QueryFresh
    
#endif

    virtual void Reset(){};

};


#endif