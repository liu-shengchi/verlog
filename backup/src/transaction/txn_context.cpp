#include "txn_context.h"


#include "txn_id.h"
#include "txn_thread.h"
#include "client.h"

#include "workload.h"

#include "access_obj.h"
#include "am_strategy.h"
#include "tuple_ver_chain_am.h"
#include "log_index_ver_chain_am.h"
#include "queryfresh_am.h"
#include "apply_writes_on_demand_am.h"

#include "statistic_manager.h"
#include "snapshot_manager.h"

#include "global_ts_comm.h"
#include "comm_manager.h"

#include "string.h"



TxnContext::TxnContext()
{
    txn_thread_ = nullptr;
    txn_id_     = nullptr;
    cur_txn_    = nullptr;

    read_ts_    = 0;

    am_strategy_ = nullptr;

    for (size_t i = 0; i < g_log_buffer_num; i++)
    {
        accessing_logbuf_[i] = false;
        accessing_lsn_[i]    = 0;
    }

    txn_state_ = TXN_STATE::TXN_NULL;


    time_breakdown_frequency_count_ = 0;
    is_time_breakdown_sample_       = false;

    micro_frequency_count_ = 0;
    is_micro_sample_       = false;


#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    is_applying_  = false;
    apply_finish_ = false;
#endif

}

TxnContext::~TxnContext()
{

}


// void TxnContext::InitContext(TxnThread* txn_thread, AMStrategy* am_strategy)
// {
//     txn_thread_ = txn_thread;

// #if   AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
//     am_strategy_ = dynamic_cast<TupleVerChainAM*>(am_strategy);
// #elif AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
//     am_strategy_ = dynamic_cast<LogIndexVerChainAM*>(am_strategy);
// #elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
//     am_strategy_ = dynamic_cast<QueryFreshAM*>(am_strategy);
// #elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
//     am_strategy_ = dynamic_cast<ApplyWritesOnDemandAM*>(am_strategy);
// #endif
    
// }


void TxnContext::InitContext(Client* client, AMStrategy* am_strategy)
{
    client_ = client;

#if   AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM
    am_strategy_ = dynamic_cast<TupleVerChainAM*>(am_strategy);
#elif AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    am_strategy_ = dynamic_cast<LogIndexVerChainAM*>(am_strategy);
#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    am_strategy_ = dynamic_cast<QueryFreshAM*>(am_strategy);
#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    am_strategy_ = dynamic_cast<ApplyWritesOnDemandAM*>(am_strategy);
#endif
}


void TxnContext::SetTxnThread(TxnThread* txn_thread)
{
    txn_thread_ = txn_thread;
}


// void TxnContext::InitTxn(Transaction* txn, Timestamp read_ts)
// {
//     txn_id_  = txn->GetTxnIdentifier();
//     cur_txn_ = txn;
//     cur_txn_->SetTxnContext(this);

//     read_ts_   = read_ts;
// }



void TxnContext::InitTxn(Transaction* txn)
{
    txn_id_  = txn->GetTxnIdentifier();
    cur_txn_ = txn;
    cur_txn_->SetTxnContext(this);
}



RC TxnContext::AccessTuple(AccessObj* access_obj, const TableID table_id, ShardID shard_id, PrimaryKey primary_key, TupleData tuple_data)
{
    RC rc = RC::RC_OK;
    
    // if (!am_strategy_->GetTupleData(access_obj, table_id, tuple_data, visible_ts_, txn_thread_->txn_thread_id_))
    if (!am_strategy_->GetTupleData(access_obj, table_id, tuple_data, this, shard_id, primary_key))
    {
        rc = RC_NULL;
    }

    return rc;
}



RC TxnContext::ExecuteTxn()
{
    RC   rc           = RC::RC_OK;
    bool continue_exe = true; 

    while (true)
    {
        switch (txn_state_)
        {
        case TXN_STATE::TXN_NULL:
            { 
                ClientStateType client_state = client_->GetClientState();

                switch (client_state)
                {
                case ClientStateType::CLIENT_FREE:
                    {
                        //客户端不存在等待执行的事务，退出
                        rc = RC_NULL;

                        continue_exe = false;
                    }
                    break;

                case ClientStateType::CLIENT_WAITING:
                    {
                        // client存在等待执行的事务
                        InitTxn(client_->GetWaitingTxn());

                        txn_state_ = TXN_STATE::TXN_BEGIN;

                        client_->SetClientState(ClientStateType::CLIENT_BUSY);

                        continue_exe = true; // 继续执行
                    }
                    break;
                
                case ClientStateType::CLIENT_BUSY:
                    {
                        printf("error! txn & client state: TXN_NULL & CLIENT_BUSY! TxnContext::ExecuteTxn \n");
                        exit(0);
                    }
                    break;

                default:
                    {
                        printf("error! undefined client state: %ld! TxnContext::ExecuteTxn \n", (uint64_t)client_->GetClientState());
                        exit(0);
                    }
                    break;
                }
            }
            break;

        case TXN_STATE::TXN_BEGIN:
            {
                //获取只读一致时间戳
                GET_CLOCK_TIME(fetch_read_ts_start_);

                g_snapshot_manager->BeforeFetchCTS(client_->client_id_);


            #if     STRONGLY_CONSISTENT_READ

                GlobalTSComm* global_ts_comm = g_comm_manager->global_ts_comm_;
                //设置TxnContext * Txn
                read_ts_ = global_ts_comm->FetchGlobalCTS(cur_txn_->arrive_ct_);
                cur_txn_->read_ts_ = read_ts_;

                g_statistic_manager->VisibilityGap(client_->client_id_, read_ts_ - g_visible_ts);

            #else

                cur_txn_->read_ts_ = g_visible_ts;

            #endif


                g_snapshot_manager->AfterFetchCTS(client_->client_id_, read_ts_);

                GET_CLOCK_TIME(fetch_read_ts_end_);


                //事务进入等待备副本构造强一致快照的状态
                txn_state_ = TXN_STATE::TXN_WAITING;
                wait_scr_snapshot_start_ = fetch_read_ts_end_;   //设置等待快照开始时间

                continue_exe = true; // 进入下一个状态后继续执行
            }
            break;

        case TXN_STATE::TXN_WAITING:
            {
                if (read_ts_ >= g_visible_ts)
                {
                    // 只读事务需要等待备副本构造强一致读取的快照
                    rc = RC_WAIT;
                    
                    continue_exe = false;
                }
                else
                {
                    // 备副本快照满足强一致读要求，开始执行只读事务
                    txn_state_ = TXN_STATE::TXN_RUNNING;

                    GET_CLOCK_TIME(wait_scr_snapshot_end_);
                    exec_txn_start_ = wait_scr_snapshot_end_;

                    continue_exe = true;
                }
            }
            break;

        case TXN_STATE::TXN_RUNNING:
            {

                /* 
                 * 目前的实现，开始执行事务后，直到事务执行结束，
                 * 该client会始终独占该执行线程。
                 */

            #if     TIME_BREAKDOWN_ANALYTIC
                ClockTime execute_read_txn_begin_time_;
                ClockTime execute_read_txn_end_time_;

                time_breakdown_frequency_count_++;
                if ((time_breakdown_frequency_count_ % g_time_breakdown_sample_frequency == 0) && g_system_state == TESTING_STATE)
                {
                    //抽样！
                    is_time_breakdown_sample_ = true;

                    GET_CLOCK_TIME(execute_read_txn_begin_time_);
                }
            #endif


            #if     MICRO_STATISTIC_ANALYTIC
                micro_frequency_count_++;
                if ((micro_frequency_count_ % g_micro_sample_frequency == 0) && g_system_state == TESTING_STATE)
                {
                    //抽样！
                    is_micro_sample_ = true;
                }

                if (IsMicroStatisticSample())
                {
                    g_statistic_manager->MicroParameterStatc(GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_TXN_COUNT_T, 1);
                }
            #endif



            #if   SIMULATE_WORKLOAD  == true
                int count = 0;
                for (uint64_t i = 0; i < g_think_time; i++)
                {
                    count++;
                }
            #else 
                cur_txn_->RunTxn();
            #endif


            #if     TIME_BREAKDOWN_ANALYTIC
                if (IsTimeBreakdownSample())
                {
                    GET_CLOCK_TIME(execute_read_txn_end_time_);

                    g_statistic_manager->TimeBreakdownStatc(GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_TXN_TIME_T, execute_read_txn_end_time_ - execute_read_txn_begin_time_);

                    //结束抽样！
                    is_time_breakdown_sample_ = false;
                }
            #endif

                txn_state_ = TXN_STATE::TXN_COMMITED;

                continue_exe = true;
            }
            break;

        case TXN_STATE::TXN_COMMITED:
            {
                GET_CLOCK_TIME(exec_txn_end_);

                //事务提交
                if (g_system_state == SystemState::TESTING_STATE)
                {
                    g_statistic_manager->TxnCommit(client_->client_id_);
                    g_statistic_manager->FetchRTSTime(client_->client_id_,    fetch_read_ts_end_ - fetch_read_ts_start_);
                    g_statistic_manager->WaitSCRSnapshot(client_->client_id_, wait_scr_snapshot_end_ - wait_scr_snapshot_start_);
                    g_statistic_manager->ExecuteTxnTime(client_->client_id_,  exec_txn_end_ - exec_txn_start_);
                }

                delete cur_txn_;
                ResetContext();

                rc = RC_COMMIT;

                client_->SetClientState(ClientStateType::CLIENT_FREE);

                txn_state_ = TXN_STATE::TXN_NULL;
                continue_exe = false;
            }
            break;

        case TXN_STATE::TXN_ABORTED:
            {
                printf("no uesd txn state! TxnContext::ExecuteTxn \n");
            }
            break;
    
        default:
            
            break;
        }
        
        if (!continue_exe)
        {
            break;
        }
    }
    
    return rc;
}




RC TxnContext::RunTxn()
{
    return cur_txn_->RunTxn();
}

RC TxnContext::FinishTxn(RC rc)
{
    return rc;
}


void TxnContext::ResetContext()
{
    /**** 事务 ****/
    txn_id_ = nullptr;
    cur_txn_ = nullptr;
}


ThreadID TxnContext::GetExecuteThreadID()
{
    return txn_thread_->GetTxnThreadID();
}


bool TxnContext::IsTimeBreakdownSample()
{
    return is_time_breakdown_sample_;
}

bool TxnContext::IsMicroStatisticSample()
{
    return is_micro_sample_;
}


#if   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
void TxnContext::StartApplying()
{
    is_applying_ = true;
}

void TxnContext::EndApplying()
{
    is_applying_ = false;
}

bool TxnContext::isApplying()
{
    return is_applying_;
}


void TxnContext::WaitApplyWrite()
{
    apply_finish_ = false;
}

void TxnContext::FinishApplyWrite()
{
    apply_finish_ = true;
}

bool TxnContext::isApplyFinish()
{
    return apply_finish_;
}
#endif
