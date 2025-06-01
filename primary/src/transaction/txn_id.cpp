#include "txn_id.h"


/****** TxnIdentifier ******/

TxnIdentifier::TxnIdentifier(ThreadID thread_id, uint64_t local_txn_id)
{
    thread_id_      = thread_id;
    local_txn_id_ = local_txn_id;
}

TxnIdentifier::~TxnIdentifier()
{

}