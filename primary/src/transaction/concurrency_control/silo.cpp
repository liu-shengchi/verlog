#include "silo.h"




RC SiloStrategy::AccessTuple(TxnState* txn_state, AccessType access_type, TableID table_id, Tuple* origin_tuple, Tuple* &return_tuple)
{
    RC rc = RC::RC_COMMIT;

    return rc;
}

RC SiloStrategy::BeginTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    return rc;
}



RC SiloStrategy::PrepareTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    return rc;
}


RC SiloStrategy::CommitTxn(TxnState* txn_state)
{
    RC rc = RC::RC_COMMIT;

    return rc;
}


RC SiloStrategy::AbortTxn(TxnState* txn_state)
{
    RC rc = RC::RC_OK;

    return rc;
}