#include "client.h"

#include "txn_context.h"

#include "log_manager.h"
#include "log_buffer.h"


Client::Client(ClientID client_id)
{
    client_id_   = client_id;

    txn_context_ = new TxnContext();
    txn_context_->InitContext(this, g_am_strategy_);

    client_state_     = ClientStateType::CLIENT_FREE;
    waiting_exec_txn_ = nullptr;

    for (LogBufID log_buf_id = 0; log_buf_id < g_log_buffer_num; log_buf_id++)
    {
        LogBuffer* log_buf = g_log_manager->GetLogBuffer(log_buf_id);
        log_buf->txn_contexts_[client_id] = txn_context_;
    }
}

Client::~Client()
{
}


bool Client::IsFree()
{
    return (client_state_ == ClientStateType::CLIENT_FREE);   
}

bool Client::IsWaiting()
{
    return (client_state_ == ClientStateType::CLIENT_WAITING);
}

bool Client::IsBusy()
{
    return (client_state_ == ClientStateType::CLIENT_BUSY);
}


void Client::SetClientState(ClientStateType client_state)
{
    COMPILER_BARRIER
    client_state_ = client_state;
    COMPILER_BARRIER
}


void Client::AddNewTxn(Transaction* new_txn)
{
    // queue_lock_.GetLatch();

    // if (wait_txn_queue_.empty() || wait_txn_queue_.size() < g_waiting_txn_queue_capacity)
    // {
    //     wait_txn_queue_.push(new_txn);
    //     queue_lock_.ReleaseLatch();
    //     return true;
    // }
    // else
    // {
    //     queue_lock_.ReleaseLatch();
    //     return false;
    // }

    waiting_exec_txn_ = new_txn;
    // COMPILER_BARRIER
    // client_state_ = ClientStateType::CLIENT_WAITING;
}


TxnContext* Client::GetTxnContext()
{
    return txn_context_;
}

Transaction* Client::GetWaitingTxn()
{
    return waiting_exec_txn_;
}

ClientStateType Client::GetClientState()
{
    return client_state_;
}

void Client::AssociateTxnThread(TxnThread* txn_thread)
{
    txn_thread_  = txn_thread;
    txn_context_->SetTxnThread(txn_thread_);
}