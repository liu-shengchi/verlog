#include "db_spin_lock.h"




DBSpinLock::DBSpinLock()
{
#if   SPIN_LOCK_TYPE == SPIN_LOCK_PTHREAD
    pthread_spin_init(&spin_lock_, PTHREAD_PROCESS_PRIVATE);
#elif SPIN_LOCK_TYPE == SPIN_LOCK_LATCH
    latch_ = false;
#endif
}

DBSpinLock::~DBSpinLock()
{
#if   SPIN_LOCK_TYPE == SPIN_LOCK_PTHREAD
    pthread_spin_destroy(&spin_lock_);
#elif SPIN_LOCK_TYPE == SPIN_LOCK_LATCH
    
#endif
}

void DBSpinLock::GetSpinLock()
{
#if   SPIN_LOCK_TYPE == SPIN_LOCK_PTHREAD
    pthread_spin_lock(&spin_lock_);
#elif SPIN_LOCK_TYPE == SPIN_LOCK_LATCH
    while (!ATOM_CAS(latch_, false, true)) { ; }
    // ATOM_CAS(latch_, false, true);
#endif

}

void DBSpinLock::ReleaseSpinLock()
{
#if   SPIN_LOCK_TYPE == SPIN_LOCK_PTHREAD
    pthread_spin_unlock(&spin_lock_);
#elif SPIN_LOCK_TYPE == SPIN_LOCK_LATCH
    ATOM_CAS(latch_, true, false);
#endif
}


