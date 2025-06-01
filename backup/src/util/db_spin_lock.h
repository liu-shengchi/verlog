#ifndef UTIL_DB_SPIN_LOCK_H_
#define UTIL_DB_SPIN_LOCK_H_

#include "config.h"
#include "pthread.h"

class DBSpinLock
{
private:

#if   SPIN_LOCK_TYPE == SPIN_LOCK_PTHREAD
    pthread_spinlock_t spin_lock_;
#elif SPIN_LOCK_TYPE == SPIN_LOCK_LATCH
    bool               latch_;
#endif


public:
    DBSpinLock();
    ~DBSpinLock();

    void GetSpinLock();
    void ReleaseSpinLock();

};




#endif