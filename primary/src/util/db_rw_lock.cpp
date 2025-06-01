#include "db_rw_lock.h"


DBrwLock::DBrwLock()
{
#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    write_lock_ = false;
    read_cnt_   = 0;
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_init(&p_rw_lock_, NULL);
#endif
}

DBrwLock::~DBrwLock()
{
}


void DBrwLock::GetReadLock()
{
#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    // while (true)
    // {
    //     while (!ATOM_CAS(write_lock_, false, false)) {}   
    //     COMPILER_BARRIER
    //     ATOM_ADD(read_cnt_, 1);
    //     COMPILER_BARRIER
    //     if (!ATOM_CAS(write_lock_, false, false))
    //     {
    //         ATOM_SUB(read_cnt_, 1);
    //         continue;
    //     }
    //     else
    //     {
    //         break;
    //     }
    // }

    while (true)
    {
        while (write_lock_ != false)
            ;
        ATOM_ADD(read_cnt_, 1);
        COMPILER_BARRIER
        if (write_lock_ != false)
        {
            ATOM_SUB(read_cnt_, 1);
            continue;
        }
        else
        {
            break;
        }
    }
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_rdlock(&p_rw_lock_);
#endif

}

void DBrwLock::ReleaseReadLock()
{
#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    ATOM_SUB(read_cnt_, 1);
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_unlock(&p_rw_lock_);
#endif

}

void DBrwLock::GetWriteLock()
{
#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    // while (!ATOM_CAS(write_lock_, false, true)) {}
    // COMPILER_BARRIER
    // while (!ATOM_CAS(read_cnt_, 0, 0)) {}

    while (!ATOM_CAS(write_lock_, false, true)) {}
    COMPILER_BARRIER
    while (read_cnt_ != 0)
        ;
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_wrlock(&p_rw_lock_);
#endif
    
}

void DBrwLock::ReleaseWriteLock()
{
#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    // ATOM_CAS(write_lock_, true, false);
    write_lock_ = false;
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_unlock(&p_rw_lock_);
#endif

}

