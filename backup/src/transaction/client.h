#ifndef   CLIENT_H_
#define   CLIENT_H_


#include "config.h"

#include "db_latch.h"

#include "queue"

using namespace std;


class Transaction;
class TxnThread;
class TxnContext;


/* 
 * 客户端
 *
 * 一个客户端仅有一个执行线程执行事务
 * 一个执行线程可以执行多个客户端的事务
 * 
 * 每个客户端均持有一个事务上下文。
 */

class Client
{
private:
    
    ClientID                 client_id_;    

    TxnThread*               txn_thread_;

    // DBLatch                  queue_lock_;
    // std::queue<Transaction*> wait_txn_queue_;

    volatile ClientStateType client_state_;
    Transaction*             waiting_exec_txn_;

    TxnContext*  txn_context_;

    friend class TxnContext;


public:
    Client(ClientID client_id);
    ~Client();

    bool IsFree();
    bool IsWaiting();
    bool IsBusy();

    void SetClientState(ClientStateType client_state);

    void AddNewTxn(Transaction* new_txn);

    TxnContext*  GetTxnContext();
    Transaction* GetWaitingTxn();

    ClientStateType GetClientState();

    void AssociateTxnThread(TxnThread* txn_thread);

};



#endif