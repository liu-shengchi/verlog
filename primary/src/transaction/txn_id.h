#ifndef TRANSACTION_TXN_ID_H_
#define TRANSACTION_TXN_ID_H_

#include "config.h"


class TxnIdentifier
{
private:

public:
    TxnIdentifier(ThreadID thread_id, uint64_t local_txn_id);
    ~TxnIdentifier();

    ThreadID GetThreadId(){ return thread_id_; };
    uint64_t GetLocalTxnId(){ return local_txn_id_; };

    ThreadID thread_id_;
    uint64_t local_txn_id_;
};


#endif
