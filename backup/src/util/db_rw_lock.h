#ifndef UTIL_DB_RW_LOCK_H_
#define UTIL_DB_RW_LOCK_H_

#include "config.h"

#include "pthread.h"

class DBrwLock
{
private:

#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    bool     volatile write_lock_;
    uint64_t volatile read_cnt_;
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_t p_rw_lock_;
#endif

public:
    DBrwLock();
    ~DBrwLock();

    void GetReadLock();
    void ReleaseReadLock();

    void GetWriteLock();
    void ReleaseWriteLock();

};


#endif