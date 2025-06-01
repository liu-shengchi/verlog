#include "txn_id.h"


/****** TxnIdentifier ******/

TxnIdentifier::TxnIdentifier(ClientID client_id, uint64_t local_txn_id)
{
    client_id_    = client_id;
    local_txn_id_ = local_txn_id;
}

TxnIdentifier::~TxnIdentifier()
{

}