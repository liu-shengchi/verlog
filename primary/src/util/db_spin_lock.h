#ifndef UTIL_DB_SPIN_LOCK_H_
#define UTIL_DB_SPIN_LOCK_H_

#include "pthread.h"

class DBSpinLock
{
private:
    
    pthread_spinlock_t spin_lock_;

public:
    DBSpinLock();
    ~DBSpinLock();

    void GetSpinLock();
    void ReleaseSpinLock();

};




#endif