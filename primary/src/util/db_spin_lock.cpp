#include "db_spin_lock.h"




DBSpinLock::DBSpinLock()
{
    pthread_spin_init(&spin_lock_, PTHREAD_PROCESS_PRIVATE);

}

DBSpinLock::~DBSpinLock()
{
}

void DBSpinLock::GetSpinLock()
{
    pthread_spin_lock(&spin_lock_);
}

void DBSpinLock::ReleaseSpinLock()
{
    pthread_spin_unlock(&spin_lock_);
}


