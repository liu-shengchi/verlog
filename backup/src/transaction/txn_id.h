#ifndef TRANSACTION_TXN_ID_H_
#define TRANSACTION_TXN_ID_H_

#include "config.h"



class TxnIdentifier
{
private:
    ClientID client_id_;
    uint64_t local_txn_id_;

public:
    TxnIdentifier(ClientID client_id, uint64_t local_txn_id);
    ~TxnIdentifier();

    void SetTxnId(ClientID client_id, uint64_t local_txn_id) {client_id_ = client_id; local_txn_id_ = local_txn_id;};

    ClientID GetClientId()   { return client_id_;    };
    uint64_t GetLocalTxnId() { return local_txn_id_; };
};


#endif
