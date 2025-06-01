#include "db_latch.h"



DBLatch::DBLatch()
{
    latch_ = false;
}

DBLatch::~DBLatch()
{
}

void DBLatch::GetLatch()
{
    while (!ATOM_CAS(latch_, false, true)) { ; }
}

void DBLatch::ReleaseLatch()
{
    ATOM_CAS(latch_, true, false);
}
