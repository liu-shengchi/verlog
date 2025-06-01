#ifndef UTIL_DB_LATCH_H_
#define UTIL_DB_LATCH_H_

#include "global.h"


class DBLatch
{
private:
    
    bool volatile latch_;

public:
    DBLatch();
    ~DBLatch();

    void GetLatch();
    void ReleaseLatch();
    
};



#endif