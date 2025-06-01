#include "statistic_manager.h"

#include "global.h"

#include "client_statistic.h"
#include "replay_thread_statistic.h"
#include "execute_thread_statistic.h"
#include "reclaim_thread_statistic.h"

#include "util_function.h"

#include "stdio.h"


StatisticManager::StatisticManager()
{
    for (uint64_t client_id = 0; client_id < g_client_total_num; client_id++)
    {
        client_stats_[client_id] = new ClientStatistic();
    }

    for (uint64_t replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_thread_stats_[replay_thread_id] = new ReplayThreadStatistic();
    }

    for (uint64_t execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_thread_stats_[execute_thread_id] = new ExecuteThreadStatistic();
    }

    for (uint64_t reclaim_thread_id = 0; reclaim_thread_id < g_reclaim_thread_num; reclaim_thread_id++)
    {
        reclaim_thread_stats_[reclaim_thread_id] = new ReclaimThreadStatistic();
    }
    
}

StatisticManager::~StatisticManager()
{

    for (uint64_t client_id = 0; client_id < g_client_total_num; client_id++)
        delete client_stats_[client_id];

    for (uint64_t  i = 0; i < g_replay_thread_num; i++)
        delete replay_thread_stats_[i];

    for (uint64_t  i = 0; i < g_txn_thread_num; i++)
        delete execute_thread_stats_[i];
}


void StatisticManager::Reset()
{
    for (uint64_t client_id = 0; client_id < g_client_total_num; client_id++)
        client_stats_[client_id]->ReSet();
}


void StatisticManager::ClientStart(ClientID client_id)
{
    ClientStatistic* client_stat = client_stats_[client_id];
    GET_CLOCK_TIME(client_stat->client_start_time_); 
}

void StatisticManager::ClientEnd(ClientID client_id)
{
    ClientStatistic* client_stat = client_stats_[client_id];
    GET_CLOCK_TIME(client_stat->client_end_time_);
}

void StatisticManager::TxnCommit(ClientID client_id)
{
    ClientStatistic* client_stat = client_stats_[client_id];
    client_stat->txn_amount_++;
    client_stat->txn_commit_++;
}


void StatisticManager::TxnAbort(ClientID client_id)
{
    ClientStatistic * client_stat = client_stats_[client_id];
    client_stat->txn_amount_++;
    client_stat->txn_abort_++;
}


void StatisticManager::FetchRTSTime(ClientID client_id, ClockTime fetch_rts_time)
{
    ClientStatistic * client_stat = client_stats_[client_id];
    client_stat->fetch_rts_time_ += fetch_rts_time;
}


void StatisticManager::WaitSCRSnapshot(ClientID client_id, ClockTime wait_time)
{
    ClientStatistic * client_stat = client_stats_[client_id];
    client_stat->wait_scr_snapshot_time_ += wait_time;
}


void StatisticManager::ExecuteTxnTime(ClientID client_id, ClockTime execute_time)
{
    ClientStatistic * client_stat = client_stats_[client_id];
    client_stat->exec_txn_time_  += execute_time;
}


void StatisticManager::VisibilityGap(ClientID client_id, Timestamp gap)
{
    ClientStatistic * client_stat = client_stats_[client_id];
    client_stat->visibility_gap_  += gap;
}


void StatisticManager::TimeBreakdownStatc(ThreadID thread_id, DBThreadType thread_type, TimeBreakdownType time_type, ClockTime time)
{
    switch (time_type)
    {
    case REPLAY_WRITE_TXN_TIME_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                // 采样次数+1
                replay_thread_stats_[thread_id]->time_break_down_sample_count_++;

                replay_thread_stats_[thread_id]->replay_write_txn_time_ += time;

                replay_thread_stats_[thread_id]->replay_write_txn_time_array_[replay_thread_stats_[thread_id]->time_break_down_sample_count_] = time;
            }
            else
            {
                printf("error! thread_type error! time_type == REPLAY_WRITE_TXN_TIME_T! \n");
                exit(0);
            }
        }
        break;
    
    case REPLAY_IDLE_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->replay_idle_time_ += time;

                replay_thread_stats_[thread_id]->replay_idle_time_array_[replay_thread_stats_[thread_id]->time_break_down_sample_count_] = time;
            }
            else
            {
                printf("error! thread_type error! time_type == REPLAY_IDLE_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case REPLAY_LOAD_ANALYSIS_LOG_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->load_analysis_log_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == REPLAY_LOAD_ANALYSIS_LOG_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case REPLAY_PAYLOAD_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->replay_payload_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == REPLAY_PAYLOAD_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case EALA_REPLAY_ACCESS_KV_INDEX_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->eala_access_kv_index_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == EALA_REPLAY_ACCESS_KV_INDEX_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case OA_REPLAY_RECORD_WRITE_INFO_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->oa_record_write_info_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == OA_REPLAY_RECORD_WRITE_INFO_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case FA_REPLAY_CREATE_VERSION_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->fa_create_version_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == FA_REPLAY_CREATE_VERSION_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case VL_MAINTAIN_LOG_CHAIN_TIME_T: 
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->vl_maintain_log_chain_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == VL_MAINTAIN_LOG_CHAIN_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case EXECUTE_READ_TXN_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->execute_read_txn_ += time;

                // 采样次数+1
                execute_thread_stats_[thread_id]->time_break_down_sample_count_++;
            }
            else
            {
                printf("error! thread_type error! time_type == EXECUTE_READ_TXN_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case EXECUTE_READ_REQUEST_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->execute_read_request_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == EXECUTE_READ_REQUEST_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case EXECUTE_TXN_PAYLOAD_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->execute_txn_payload_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == EXECUTE_TXN_PAYLOAD_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->oa_fetch_unapplied_write_info_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == OA_EXECUTE_FETCH_UNAPPLIED_WRITES_INFO_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->oa_load_load_from_log_store_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == OA_EXECUTE_LOAD_LOG_FROME_LOG_STORE_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case OA_EXECUTE_CREATE_VERSION_TIME_T: 
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->oa_create_version_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == OA_EXECUTE_CREATE_VERSION_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case VL_RECLAIM_WRITE_TXN_TIME_T:
        {
            if (thread_type == RECLAIM_THREAD_T)
            {
                // 采样次数+1
                reclaim_thread_stats_[thread_id]->reclaim_write_txn_sample_count_++;

                reclaim_thread_stats_[thread_id]->reclaim_write_txn_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == VL_RECLAIM_WRITE_TXN_TIME_T! \n");
                exit(0);
            }
        }
        break;

    case VL_RECLAIM_IDLE_TIME_T:
        {
            if (thread_type == RECLAIM_THREAD_T)
            {
                reclaim_thread_stats_[thread_id]->reclaim_idle_time_ += time;
            }
            else
            {
                printf("error! thread_type error! time_type == VL_RECLAIM_IDLE_TIME_T! \n");
                exit(0);
            }
        }
        break;

    default:
        break;
    }
}



void StatisticManager::MicroParameterStatc(ThreadID thread_id, DBThreadType thread_type, MicronStatisticType micro_type, uint64_t count)
{


    switch (micro_type)
    {
    case REPLAY_WRITE_TXN_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->micro_statc_sample_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == REPLAY_WRITE_TXN_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case EA_REPLAY_ACCESS_KV_INDEX_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->ea_replay_access_kv_index_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == EA_REPLAY_ACCESS_KV_INDEX_COUNT_T! \n");
                exit(0);
            }
        }
        break;
    
    case EA_REPLAY_CREATE_VERSION_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->ea_replay_create_version_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == EA_REPLAY_CREATE_VERSION_COUNT_T! \n");
                exit(0);
            }
        }
        break;
        
    case LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->la_replay_record_unapplied_write_info_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_REPLAY_RECORD_UNAPPLIED_WRITE_INFO_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case LA_REPLAY_ACCESS_KV_INDEX_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->la_replay_access_kv_index_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_REPLAY_ACCESS_KV_INDEX_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case VL_REPLAY_ACCESS_KV_INDEX_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->vl_replay_access_kv_index_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == VL_REPLAY_ACCESS_KV_INDEX_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case VL_FAST_REPLAY_PATH_COUNT_T:
        {
            if (thread_type == REPLAY_THREAD_T)
            {
                replay_thread_stats_[thread_id]->vl_fast_replay_path_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == VL_FAST_REPLAY_PATH_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case EXECUTE_READ_TXN_COUNT_T:
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->micro_statc_sample_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == EXECUTE_READ_TXN_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T:
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->la_execute_fetch_unapplied_write_info_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_EXECUTE_FETCH_UNAPPLIED_WRTIE_INFO_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case LA_EXECUTE_LOAD_LOG_COUNT_T:
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->la_execute_load_log_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_EXECUTE_LOAD_LOG_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case LA_EXECUTE_ACCESS_DISK_COUNT_T:
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->la_execute_access_disk_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_EXECUTE_ACCESS_DISK_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case LA_EXECUTE_CREATE_VERSION_COUNT_T:
        {
            if (thread_type == TXN_THREAD_T)
            {
                execute_thread_stats_[thread_id]->la_execute_create_version_count_ += count;
            }
            else
            {
                printf("error! thread_type error! time_type == LA_EXECUTE_CREATE_VERSION_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case RECLAIM_WRITE_TXN_COUNT_T:
        {
            if (thread_type == RECLAIM_THREAD_T)
            {
                reclaim_thread_stats_[thread_id]->reclaim_write_txn_sample_count_ += count;
            }
            else
            {
                printf("error! thread_type error! micro_type == RECLAIM_WRITE_TXN_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case VL_INVISIBLE_VERSION_COUNT_T:
        {
            if (thread_type == RECLAIM_THREAD_T)
            {
                reclaim_thread_stats_[thread_id]->vl_invisible_version_count_ += count;
            }
            else
            {
                printf("error! thread_type error! micro_type == VL_INVISIBLE_VERSION_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    case VL_MATERIAL_VERSION_COUNT_T:
        {
            if (thread_type == RECLAIM_THREAD_T)
            {
                reclaim_thread_stats_[thread_id]->vl_material_version_count_ += count;
            }
            else
            {
                printf("error! thread_type error! micro_type == VL_MATERIAL_VERSION_COUNT_T! \n");
                exit(0);
            }
        }
        break;

    default:
        break;
    }

}




void StatisticManager::PrintStatResult()
{
    uint64_t txn_amount = 0;
    uint64_t txn_commit = 0;
    uint64_t txn_abort  = 0;

    ClockTime fetch_rts_time         = 0;
    ClockTime wait_scr_snapshot_time = 0;
    ClockTime exec_txn_time          = 0;

    Timestamp visibility_gap         = 0;

    double test_time = (end_time_ - start_time_) / 1000000000.0;

    for (ClientID client_id = 0; client_id < g_client_total_num; client_id++)
    {
        ClientStatistic * client_stat = client_stats_[client_id];
        printf("client %d throughput: %lf \n", client_id, client_stat->txn_commit_ / test_time);

        txn_amount += client_stat->txn_amount_;
        txn_commit += client_stat->txn_commit_;
        txn_abort  += client_stat->txn_abort_;
        fetch_rts_time         += client_stat->fetch_rts_time_;
        wait_scr_snapshot_time += client_stat->wait_scr_snapshot_time_;
        exec_txn_time          += client_stat->exec_txn_time_;
        visibility_gap         += client_stat->visibility_gap_;
    }
    
    double txn_throughput = txn_commit / test_time;

    printf("\n\nstatistic information: \n");    
    printf("the number of txns executed:  %ld \n", txn_amount);
    printf("the number of txns committed: %ld \n", txn_commit);
    printf("the amount of txns aborted:   %ld \n", txn_abort);
    printf("the ratio of txns committed:  %.4lf \n", (double)txn_commit / txn_amount);
    printf("the ratio of txns aborted:    %.4lf \n", (double)txn_abort / txn_amount);

    printf("the time of test: %lf secs \n", test_time);
    printf("the tps of transactional throughput: %.2lf \n\n", txn_throughput);
    
    // printf("the time of get replaying txn log fails: %ld\n\n", get_replay_txn_log_fail_);

    printf("直接返回g_commit_ts: %" PRIu64 ", 等待后返回g_commit_ts: %" PRIu64 ", 等待并向主节点获取g_commit_ts: %" PRIu64 "\n", count1, count2, count3);
    printf("最后一次获取的全局时间戳: %" PRIu64 "\n", g_commit_ts);
    
    printf("sequencing phase average time:   %.2lf us\n", ((double)fetch_rts_time) / txn_commit / 1000);
    printf("waiting phase average time:      %.2lf us\n", ((double)wait_scr_snapshot_time) / txn_commit / 1000);
    printf("executing phase average time:    %.2lf us\n", ((double)exec_txn_time) / txn_commit / 1000);

    printf("visiblity gap (RTA - VTS) average per ro-txn:  %ld\n", visibility_gap / txn_amount);

    printf("\n");


#if   MICRO_STATISTICAL_DATA_ANALYTIC
#if   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM
    
    printf("访问方法: IndexLog!\n");
    printf("回放统计: \n \
            前序日志版本已回收: %" PRIu64 ", \
            前序日志版本未回放: %" PRIu64 ", \
            无前序日志版本(Insert): %" PRIu64 ", \
            前序日志版本已回放且未被回收: %" PRIu64 " \n", \
            count4, count5, count6, count7);

    printf("回收统计: \n \
            创建的元组版本数量: %" PRIu64 " \n", \
            count8);

    printf("访问统计: \n \
            可见版本在日志版本区间: %" PRIu64 ", \
            可见版本在元组版本区间: %" PRIu64 " \n" , \
            count9, count10);

#elif AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM

    printf("访问方法: C5!\n");
    printf("创建的元组版本数量(访问索引的次数): %" PRIu64 " \n", \
            count11 * g_replay_thread_num);

#elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
    
    printf("访问方法: QueryFresh!\n");
    printf("创建元组版本的数量: %" PRIu64 ", \
            访问磁盘: %" PRIu64 "\n", count12 * g_client_total_num, count13 * g_client_total_num);

#elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    printf("创建kv版本: %" PRIu64 "\n", count12 * g_apply_thread_num);
    printf("访问磁盘: %" PRIu64 "\n",   count13 * g_apply_thread_num);
#endif


#endif




#if     MICRO_STATISTIC_ANALYTIC

  #if AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM

    printf("Micro Statistic: \n");

    printf("Replay Phase: \n");
    uint64_t replay_write_txn_count       = 0;
    uint64_t replay_access_kv_index_count = 0;
    uint64_t replay_create_version_count  = 0;
    
    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_write_txn_count       += replay_thread_stats_[replay_thread_id]->micro_statc_sample_count_;

        replay_access_kv_index_count += replay_thread_stats_[replay_thread_id]->ea_replay_access_kv_index_count_;
        replay_create_version_count  += replay_thread_stats_[replay_thread_id]->ea_replay_create_version_count_;
    }
    
    printf("access kv index count per w-txn:       %.2lf\n", replay_access_kv_index_count * 1.00 / replay_write_txn_count);
    printf("create version count count per w-txn:  %.2lf\n", replay_create_version_count * 1.00 / replay_write_txn_count);
    
    uint64_t access_kv_index_count_per_seconds = replay_access_kv_index_count * g_micro_sample_frequency / test_time;
    uint64_t create_version_count_per_seconds  = replay_create_version_count * g_micro_sample_frequency / test_time;

    printf("access kv index count per seconds:      %" PRIu64 "\n", access_kv_index_count_per_seconds);
    printf("create version count count per seconds: %" PRIu64 "\n", create_version_count_per_seconds);

    

  #elif AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

    printf("Micro Statistic: \n");

    printf("Replay Phase: \n");
    uint64_t replay_write_txn_count       = 0;
    uint64_t replay_access_kv_index_count = 0;
    uint64_t replay_record_write_info_count  = 0;
    
    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_write_txn_count       += replay_thread_stats_[replay_thread_id]->micro_statc_sample_count_;

        replay_access_kv_index_count   += replay_thread_stats_[replay_thread_id]->la_replay_access_kv_index_count_;
        replay_record_write_info_count += replay_thread_stats_[replay_thread_id]->la_replay_record_unapplied_write_info_count_;
    }
    
    printf("access kv index count per w-txn:          %.2lf\n", replay_access_kv_index_count * 1.00 / replay_write_txn_count);
    printf("record write info count count per w-txn:  %.2lf\n", replay_record_write_info_count * 1.00 / replay_write_txn_count);
    
    uint64_t access_kv_index_count_per_seconds   = replay_access_kv_index_count * g_micro_sample_frequency / test_time;
    uint64_t record_write_info_count_per_seconds = replay_record_write_info_count * g_micro_sample_frequency / test_time;

    printf("access kv index count per seconds:     %" PRIu64 "\n", access_kv_index_count_per_seconds);
    printf("record write info count per seconds:   %" PRIu64 "\n", record_write_info_count_per_seconds);
    printf("\n");

    printf("Execute Phase: \n");

    uint64_t execute_read_txn_count         = 0;
    uint64_t execute_fetch_write_info_count = 0;
    uint64_t execute_load_log_count         = 0;
    uint64_t execute_access_disk_count      = 0;
    uint64_t execute_create_version_count   = 0;
    
    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_read_txn_count       += execute_thread_stats_[execute_thread_id]->micro_statc_sample_count_;

        execute_fetch_write_info_count += execute_thread_stats_[execute_thread_id]->la_execute_fetch_unapplied_write_info_count_;
        execute_load_log_count         += execute_thread_stats_[execute_thread_id]->la_execute_load_log_count_;
        execute_access_disk_count      += execute_thread_stats_[execute_thread_id]->la_execute_access_disk_count_;
        execute_create_version_count   += execute_thread_stats_[execute_thread_id]->la_execute_create_version_count_;
    }
    
    printf("fetch write info count per r-txn: %.2lf\n", execute_fetch_write_info_count * 1.00 / execute_read_txn_count);
    printf("load log per r-txn:               %.2lf\n", execute_load_log_count * 1.00 / execute_read_txn_count);
    printf("access disk per r-txn:            %.2lf\n", execute_access_disk_count * 1.00 / execute_read_txn_count);
    printf("create version per r-txn:         %.2lf\n", execute_create_version_count * 1.00 / execute_read_txn_count);


  #elif AM_STRATEGY_TYPE == QUERY_FRESH_AM
        
    printf("Micro Statistic: \n");

    printf("Replay Phase: \n");
    uint64_t replay_write_txn_count       = 0;
    uint64_t replay_access_kv_index_count = 0;
    uint64_t replay_record_write_info_count  = 0;
    
    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_write_txn_count       += replay_thread_stats_[replay_thread_id]->micro_statc_sample_count_;

        replay_access_kv_index_count   += replay_thread_stats_[replay_thread_id]->la_replay_access_kv_index_count_;
        replay_record_write_info_count += replay_thread_stats_[replay_thread_id]->la_replay_record_unapplied_write_info_count_;
    }
    
    printf("access kv index count per w-txn:          %.2lf\n", replay_access_kv_index_count * 1.00 / replay_write_txn_count);
    printf("record write info count count per w-txn:  %.2lf\n", replay_record_write_info_count * 1.00 / replay_write_txn_count);
    
    uint64_t access_kv_index_count_per_seconds   = replay_access_kv_index_count * g_micro_sample_frequency / test_time;
    uint64_t record_write_info_count_per_seconds = replay_record_write_info_count * g_micro_sample_frequency / test_time;

    printf("access kv index count per seconds:     %" PRIu64 "\n", access_kv_index_count_per_seconds);
    printf("record write info count per seconds:   %" PRIu64 "\n", record_write_info_count_per_seconds);

    printf("\n");

    printf("Execute Phase: \n");

    uint64_t execute_read_txn_count         = 0;
    uint64_t execute_fetch_write_info_count = 0;
    uint64_t execute_load_log_count         = 0;
    uint64_t execute_access_disk_count      = 0;
    uint64_t execute_create_version_count   = 0;
    
    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_read_txn_count       += execute_thread_stats_[execute_thread_id]->micro_statc_sample_count_;

        execute_fetch_write_info_count += execute_thread_stats_[execute_thread_id]->la_execute_fetch_unapplied_write_info_count_;
        execute_load_log_count         += execute_thread_stats_[execute_thread_id]->la_execute_load_log_count_;
        execute_access_disk_count      += execute_thread_stats_[execute_thread_id]->la_execute_access_disk_count_;
        execute_create_version_count   += execute_thread_stats_[execute_thread_id]->la_execute_create_version_count_;
    }
    
    printf("fetch write info count per r-txn: %.2lf\n", execute_fetch_write_info_count * 1.00 / execute_read_txn_count);
    printf("load log per r-txn:               %.2lf\n", execute_load_log_count * 1.00 / execute_read_txn_count);
    printf("access disk per r-txn:            %.2lf\n", execute_access_disk_count * 1.00 / execute_read_txn_count);
    printf("create version per r-txn:         %.2lf\n", execute_create_version_count * 1.00 / execute_read_txn_count);

  #elif   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM

    printf("Micro Statistic: \n");

    printf("Replay Phase: \n");
    uint64_t replay_write_txn_count       = 0;
    uint64_t replay_access_kv_index_count = 0;
    uint64_t fast_replay_path_count       = 0;

    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_write_txn_count       += replay_thread_stats_[replay_thread_id]->micro_statc_sample_count_;

        replay_access_kv_index_count += replay_thread_stats_[replay_thread_id]->vl_replay_access_kv_index_count_;
        fast_replay_path_count       += replay_thread_stats_[replay_thread_id]->vl_fast_replay_path_count_;
    }
    
    printf("access kv index count per w-txn:       %.2lf\n", replay_access_kv_index_count * 1.00 / replay_write_txn_count);
    printf("fast replay path count per w-txn:      %.2lf\n", fast_replay_path_count * 1.00 / replay_write_txn_count);


    uint64_t access_kv_index_count_per_seconds  = replay_access_kv_index_count * g_micro_sample_frequency / test_time;
    uint64_t fast_replay_path_count_per_seconds = fast_replay_path_count * g_micro_sample_frequency / test_time;
    
    printf("access kv index count per seconds:      %" PRIu64 "\n", access_kv_index_count_per_seconds);
    printf("fast replay path count per seconds:     %" PRIu64 "\n", fast_replay_path_count_per_seconds);


    printf("Reclaim Phase: \n");
    uint64_t reclaim_write_txn_count          = 0;
    uint64_t reclaim_invisible_version_count  = 0;
    uint64_t reclaim_material_version_count   = 0;

    for (ThreadID reclaim_thread_id = 0; reclaim_thread_id < g_reclaim_thread_num; reclaim_thread_id++)
    {
        reclaim_write_txn_count          += reclaim_thread_stats_[reclaim_thread_id]->reclaim_write_txn_sample_count_;

        reclaim_invisible_version_count  += reclaim_thread_stats_[reclaim_thread_id]->vl_invisible_version_count_;
        reclaim_material_version_count   += reclaim_thread_stats_[reclaim_thread_id]->vl_material_version_count_;
    }

    printf("reclaim invisible version count per w-txn:   %.2lf\n", reclaim_invisible_version_count * 1.00 / reclaim_write_txn_count);
    printf("reclaim material version count per w-txn:    %.2lf\n", reclaim_material_version_count * 1.00 / reclaim_write_txn_count);


    uint64_t reclaim_invisible_version_count_per_seconds = reclaim_invisible_version_count * g_micro_sample_frequency / test_time;
    uint64_t reclaim_material_version_count_per_seconds  = reclaim_material_version_count * g_micro_sample_frequency / test_time;
    
    printf("reclaim invisible version count per seconds:    %" PRIu64 "\n", reclaim_invisible_version_count_per_seconds);
    printf("reclaim material version count per seconds:     %" PRIu64 "\n", reclaim_material_version_count_per_seconds);


  #endif

#endif


    printf("\n\n");




#if      TIME_BREAKDOWN_ANALYTIC

    printf("Time Breakdown Analysis:\n");

  #if     AM_STRATEGY_TYPE == TUPLE_VERSION_CHAIN_AM

    ClockTime replay_sample_count           = 0;

    ClockTime replay_total_time             = 0;

    ClockTime replay_idle_time              = 0;
    ClockTime replay_write_txn_time         = 0;

    ClockTime replay_load_analysis_log_time = 0;
    ClockTime replay_payload_time           = 0;
    ClockTime replay_other_time             = 0;
    ClockTime replay_access_kv_index_time   = 0;
    ClockTime replay_create_version_time    = 0;
    


    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_sample_count += replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_;

        replay_idle_time      += replay_thread_stats_[replay_thread_id]->replay_idle_time_;
        replay_write_txn_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_;

        replay_load_analysis_log_time += replay_thread_stats_[replay_thread_id]->load_analysis_log_time_;
        replay_payload_time           += replay_thread_stats_[replay_thread_id]->replay_payload_time_;
        replay_access_kv_index_time   += replay_thread_stats_[replay_thread_id]->eala_access_kv_index_time_;
        replay_create_version_time    += replay_thread_stats_[replay_thread_id]->fa_create_version_time_;

        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_idle_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
    }

    replay_total_time = replay_idle_time + replay_write_txn_time;

    replay_other_time =   replay_write_txn_time 
                        - replay_load_analysis_log_time 
                        - replay_payload_time;

    printf("replay thread time breakdown: \n");
    printf("replay idle time ratio:          %.2lf\% \n", replay_idle_time * 1.00 / replay_total_time * 100);
    printf("replay write txn time ratio:     %.2lf\% \n", replay_write_txn_time * 1.00 / replay_total_time * 100);

    printf("replay write txn time breakdown: \n");

    //单位是 us
    double replay_write_txn_average_time         = replay_write_txn_time * 1.00 / replay_sample_count / 1000;
    double replay_load_analysis_log_average_time = replay_load_analysis_log_time * 1.00 / replay_sample_count / 1000;
    double replay_payload_average_time           = replay_payload_time * 1.00 / replay_sample_count / 1000;
    double replay_other_average_time             = replay_other_time * 1.00 / replay_sample_count / 1000;

    double replay_access_kv_index_average_time   = replay_access_kv_index_time * 1.00 / replay_sample_count / 1000;
    double replay_create_version_average_time    = replay_create_version_time * 1.00 / replay_sample_count / 1000;
    
    printf("replay write txn average time:   %.2lf \n", replay_write_txn_average_time);
    printf("log analysis time & ratio:       %.2lf us  &  %.2lf\% \n", replay_load_analysis_log_average_time, replay_load_analysis_log_average_time / replay_write_txn_average_time * 100);
    printf("replay payload time & ratio:     %.2lf us  &  %.2lf\% \n", replay_payload_average_time, replay_payload_average_time / replay_write_txn_average_time * 100);
    printf("replay other time & ratio:       %.2lf us  &  %.2lf\% \n", replay_other_average_time, replay_other_average_time / replay_write_txn_average_time * 100);
    
    printf("access kv index time & ratio:    %.2lf us  &  %.2lf\% \n", replay_access_kv_index_average_time, replay_access_kv_index_average_time / replay_write_txn_average_time * 100);
    printf("create version time & ratio:     %.2lf us  &  %.2lf\% \n", replay_create_version_average_time, replay_create_version_average_time / replay_write_txn_average_time * 100);
    

    printf("\n");


    printf("replay write txn time distribution: \n");
    // 输出回放写事务的时间分布
    for (uint64_t proportion = g_time_breakdown_distribution_proportion_start; proportion >= g_time_breakdown_distribution_proportion_end; proportion -= g_time_breakdown_distribution_proportion_gap)
    {
        ClockTime total_time = 0;

        for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
        {
            uint64_t index = (uint64_t)(replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_ * proportion * 1.00 / 100);
            total_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_[index];
        }

        printf("proportion: %ld\%, replay time: %.2lf \n", proportion, total_time / g_replay_thread_num * 1.00 / 1000);
    }

    printf("\n");




    ClockTime execute_sample_count     = 0;

    ClockTime execute_read_txn_time    = 0;
    ClockTime execute_read_req_time    = 0;
    ClockTime execute_txn_payload_time = 0;

    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_sample_count += execute_thread_stats_[execute_thread_id]->time_break_down_sample_count_;

        execute_read_txn_time += execute_thread_stats_[execute_thread_id]->execute_read_txn_;
        execute_read_req_time += execute_thread_stats_[execute_thread_id]->execute_read_request_;
    }

    // 使用非抽样数据作为事务的执行时间，是为了避免抽样中GET_CLOCK_TIME函数调用本身的用时影响事务执行时间的计算
    // execute_read_txn_time    = ((double)exec_txn_time) / txn_commit * execute_sample_count;

    execute_txn_payload_time = execute_read_txn_time - execute_read_req_time;

    // 单位 us
    double execute_read_txn_average_time    = execute_read_txn_time * 1.00 / execute_sample_count / 1000;
    double execute_read_req_average_time    = execute_read_req_time * 1.00 / execute_sample_count / 1000;
    double execute_txn_payload_average_time = execute_txn_payload_time * 1.00 / execute_sample_count / 1000;
    
    printf("execute read txn time breakdown: \n");
    printf("execute read txn average time:            %.2lf \n", execute_read_txn_average_time);
    printf("execute read req average time & ratio:    %.2lf us  &  %.2lf\% \n", execute_read_req_average_time, execute_read_req_average_time / execute_read_txn_average_time * 100);
    printf("execute txn payload average time & ratio: %.2lf us  &  %.2lf\% \n", execute_txn_payload_average_time, execute_txn_payload_average_time / execute_read_txn_average_time * 100);



  #elif   AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM

    ClockTime replay_sample_count           = 0;

    ClockTime replay_total_time             = 0;

    ClockTime replay_idle_time              = 0;
    ClockTime replay_write_txn_time         = 0;

    ClockTime replay_load_analysis_log_time = 0;
    ClockTime replay_payload_time           = 0;
    ClockTime replay_other_time             = 0;

    ClockTime replay_record_write_info_time = 0;
    ClockTime access_kv_index_time   = 0;

    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_sample_count += replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_;

        replay_idle_time      += replay_thread_stats_[replay_thread_id]->replay_idle_time_;
        replay_write_txn_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_;

        replay_load_analysis_log_time += replay_thread_stats_[replay_thread_id]->load_analysis_log_time_;
        replay_payload_time           += replay_thread_stats_[replay_thread_id]->replay_payload_time_;

        replay_record_write_info_time += replay_thread_stats_[replay_thread_id]->oa_record_write_info_time_;
        access_kv_index_time          += replay_thread_stats_[replay_thread_id]->eala_access_kv_index_time_;

        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_idle_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
    }

    replay_total_time = replay_idle_time + replay_write_txn_time;

    replay_other_time =  replay_write_txn_time 
                       - replay_load_analysis_log_time
                       - replay_payload_time;

    printf("replay thread time breakdown: \n");
    printf("replay idle time ratio:          %.2lf\% \n", replay_idle_time * 1.00 / replay_total_time * 100);
    printf("replay write txn time ratio:     %.2lf\% \n", replay_write_txn_time * 1.00 / replay_total_time * 100);

    printf("replay write txn time breakdown: \n");

    //单位是 us
    double replay_write_txn_average_time         = replay_write_txn_time * 1.00 / replay_sample_count / 1000;
    double replay_load_analysis_log_average_time = replay_load_analysis_log_time * 1.00 / replay_sample_count / 1000;
    double replay_payload_average_time           = replay_payload_time * 1.00 / replay_sample_count / 1000;
    double replay_other_average_time             = replay_other_time * 1.00 / replay_sample_count / 1000;
    double replay_record_write_info_average_time = replay_record_write_info_time * 1.00 / replay_sample_count / 1000;
    double access_kv_index_average_time          = access_kv_index_time * 1.00 / replay_sample_count / 1000;
    

    printf("replay write txn average time:   %.2lf \n", replay_write_txn_average_time);
    printf("log analysis time & ratio:       %.2lf us  &  %.2lf\% \n", replay_load_analysis_log_average_time, replay_load_analysis_log_average_time / replay_write_txn_average_time * 100);
    printf("replay payload time & ratio:     %.2lf us  &  %.2lf\% \n", replay_payload_average_time, replay_payload_average_time / replay_write_txn_average_time * 100);
    printf("replay other time & ratio:       %.2lf us  &  %.2lf\% \n", replay_other_average_time, replay_other_average_time / replay_write_txn_average_time * 100);
    printf("record write info time & ratio:  %.2lf us  &  %.2lf\% \n", replay_record_write_info_average_time, replay_record_write_info_average_time / replay_write_txn_average_time * 100);
    printf("access kv index time & ratio:    %.2lf us  &  %.2lf\% \n", access_kv_index_average_time, access_kv_index_average_time / replay_write_txn_average_time * 100);


    printf("\n");



    printf("replay write txn time distribution: \n");
    // 输出回放写事务的时间分布
    for (uint64_t proportion = g_time_breakdown_distribution_proportion_start; proportion >= g_time_breakdown_distribution_proportion_end; proportion -= g_time_breakdown_distribution_proportion_gap)
    {
        ClockTime total_time = 0;

        for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
        {
            uint64_t index = (uint64_t)(replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_ * proportion * 1.00 / 100);
            total_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_[index];
        }

        printf("proportion: %ld\%, replay time: %.2lf \n", proportion, total_time / g_replay_thread_num * 1.00 / 1000);
    }

    printf("\n");




    ClockTime execute_sample_count     = 0;

    ClockTime execute_read_txn_time    = 0;
    ClockTime execute_read_req_time    = 0;
    ClockTime execute_txn_payload_time = 0;
    ClockTime fetch_unapplied_writes_info_time = 0;
    ClockTime load_log_from_log_store_time     = 0;
    ClockTime create_version_time              = 0;


    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_sample_count += execute_thread_stats_[execute_thread_id]->time_break_down_sample_count_;

        //使用采样中获取的读事务的执行时间，包括多次GET_CLOCK_TIME的时间
        execute_read_txn_time += execute_thread_stats_[execute_thread_id]->execute_read_txn_;

        execute_read_req_time            += execute_thread_stats_[execute_thread_id]->execute_read_request_;
        fetch_unapplied_writes_info_time += execute_thread_stats_[execute_thread_id]->oa_fetch_unapplied_write_info_;
        load_log_from_log_store_time     += execute_thread_stats_[execute_thread_id]->oa_load_load_from_log_store_;
        create_version_time              += execute_thread_stats_[execute_thread_id]->oa_create_version_ - execute_thread_stats_[execute_thread_id]->oa_load_load_from_log_store_;
    }



    // 使用非抽样数据作为事务的执行时间，是为了避免抽样中GET_CLOCK_TIME函数调用本身的用时影响事务执行时间的计算
    // execute_read_txn_time    = ((double)exec_txn_time) / txn_commit * execute_sample_count;

    execute_txn_payload_time = execute_read_txn_time - execute_read_req_time;

    // 单位 us
    double execute_read_txn_average_time    = execute_read_txn_time * 1.00 / execute_sample_count / 1000;
    double execute_read_req_average_time    = execute_read_req_time * 1.00 / execute_sample_count / 1000;

    double fetch_unapplied_writes_info_average_time = fetch_unapplied_writes_info_time * 1.00 / execute_sample_count / 1000;
    double load_log_from_log_store_average_time     = load_log_from_log_store_time * 1.00 / execute_sample_count / 1000;
    double create_version_average_time              = create_version_time * 1.00 / execute_sample_count / 1000;

    double execute_txn_payload_average_time = execute_txn_payload_time * 1.00 / execute_sample_count / 1000;
    
    printf("execute read txn time breakdown: \n");
    printf("execute read txn average time:                    %.2lf \n", execute_read_txn_average_time);
    printf("execute read req average time & ratio:            %.2lf us  &  %.2lf\% \n", execute_read_req_average_time, execute_read_req_average_time / execute_read_txn_average_time * 100);
    printf("fetch unapplied writes info average time & ratio: %.2lf us  &  %.2lf\% \n", fetch_unapplied_writes_info_average_time, fetch_unapplied_writes_info_average_time / execute_read_txn_average_time * 100);
    printf("load log from log store average time & ratio:     %.2lf us  &  %.2lf\% \n", load_log_from_log_store_average_time, load_log_from_log_store_average_time / execute_read_txn_average_time * 100);
    printf("create version average time & ratio:              %.2lf us  &  %.2lf\% \n", create_version_average_time, create_version_average_time / execute_read_txn_average_time * 100);

    printf("execute txn payload average time & ratio:         %.2lf us  &  %.2lf\% \n", execute_txn_payload_average_time, execute_txn_payload_average_time / execute_read_txn_average_time * 100);


  #elif   AM_STRATEGY_TYPE == QUERY_FRESH_AM

    ClockTime replay_sample_count           = 0;

    ClockTime replay_total_time             = 0;

    ClockTime replay_idle_time              = 0;
    ClockTime replay_write_txn_time         = 0;

    ClockTime replay_load_analysis_log_time = 0;
    ClockTime replay_payload_time           = 0;
    ClockTime replay_other_time             = 0;

    ClockTime replay_record_write_info_time = 0;
    ClockTime replay_access_index_time      = 0;


    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_sample_count += replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_;

        replay_idle_time      += replay_thread_stats_[replay_thread_id]->replay_idle_time_;
        replay_write_txn_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_;

        replay_load_analysis_log_time += replay_thread_stats_[replay_thread_id]->load_analysis_log_time_;
        replay_payload_time           += replay_thread_stats_[replay_thread_id]->replay_payload_time_;
        
        replay_record_write_info_time += replay_thread_stats_[replay_thread_id]->oa_record_write_info_time_;
        replay_access_index_time      += replay_thread_stats_[replay_thread_id]->eala_access_kv_index_time_;

        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_idle_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
    }

    replay_total_time = replay_idle_time + replay_write_txn_time;

    replay_other_time =   replay_write_txn_time 
                        - replay_load_analysis_log_time 
                        - replay_payload_time;

    printf("replay thread time breakdown: \n");
    printf("replay idle time ratio:          %.2lf\% \n", replay_idle_time * 1.00 / replay_total_time * 100);
    printf("replay write txn time ratio:     %.2lf\% \n", replay_write_txn_time * 1.00 / replay_total_time * 100);

    printf("replay write txn time breakdown: \n");

    //单位是 us
    double replay_write_txn_average_time         = replay_write_txn_time * 1.00 / replay_sample_count / 1000;
    double replay_load_analysis_log_average_time = replay_load_analysis_log_time * 1.00 / replay_sample_count / 1000;
    double replay_payload_average_time           = replay_payload_time * 1.00 / replay_sample_count / 1000;
    double replay_other_average_time             = replay_other_time * 1.00 / replay_sample_count / 1000;
    double replay_record_write_info_average_time = replay_record_write_info_time * 1.00 / replay_sample_count / 1000;
    double replay_access_index_average_time      = replay_access_index_time * 1.00 / replay_sample_count / 1000;


    printf("replay write txn average time:   %.2lf \n", replay_write_txn_average_time);
    printf("log analysis time & ratio:       %.2lf us  &  %.2lf\% \n", replay_load_analysis_log_average_time, replay_load_analysis_log_average_time / replay_write_txn_average_time * 100);
    printf("replay payload time & ratio:     %.2lf us  &  %.2lf\% \n", replay_payload_average_time, replay_payload_average_time / replay_write_txn_average_time * 100);
    printf("replay other time & ratio:       %.2lf us  &  %.2lf\% \n", replay_other_average_time, replay_other_average_time / replay_write_txn_average_time * 100);
    printf("record write info time & ratio:  %.2lf us  &  %.2lf\% \n", replay_record_write_info_average_time, replay_record_write_info_average_time / replay_write_txn_average_time * 100);
    printf("access kv index time & ratio:    %.2lf us  &  %.2lf\% \n", replay_access_index_average_time, replay_access_index_average_time / replay_write_txn_average_time * 100);


    printf("\n");


    printf("replay write txn time distribution: \n");
    // 输出回放写事务的时间分布
    for (uint64_t proportion = g_time_breakdown_distribution_proportion_start; proportion >= g_time_breakdown_distribution_proportion_end; proportion -= g_time_breakdown_distribution_proportion_gap)
    {
        ClockTime total_time = 0;

        for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
        {
            uint64_t index = (uint64_t)(replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_ * proportion * 1.00 / 100);
            total_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_[index];
        }

        printf("proportion: %ld\%, replay time: %.2lf \n", proportion, total_time / g_replay_thread_num * 1.00 / 1000);
    }

    printf("\n");



    ClockTime execute_sample_count     = 0;

    ClockTime execute_read_txn_time    = 0;
    ClockTime execute_read_req_time    = 0;
    ClockTime execute_txn_payload_time = 0;
    ClockTime fetch_unapplied_writes_info_time = 0;
    ClockTime load_log_from_log_store_time     = 0;
    ClockTime create_version_time              = 0;


    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_sample_count += execute_thread_stats_[execute_thread_id]->time_break_down_sample_count_;

        //使用采样中获取的读事务的执行时间，包括多次GET_CLOCK_TIME的时间
        execute_read_txn_time += execute_thread_stats_[execute_thread_id]->execute_read_txn_;

        execute_read_req_time            += execute_thread_stats_[execute_thread_id]->execute_read_request_;
        fetch_unapplied_writes_info_time += execute_thread_stats_[execute_thread_id]->oa_fetch_unapplied_write_info_;
        load_log_from_log_store_time     += execute_thread_stats_[execute_thread_id]->oa_load_load_from_log_store_;
        create_version_time              += execute_thread_stats_[execute_thread_id]->oa_create_version_ - execute_thread_stats_[execute_thread_id]->oa_load_load_from_log_store_;
    }



    // 使用非抽样数据作为事务的执行时间，是为了避免抽样中GET_CLOCK_TIME函数调用本身的用时影响事务执行时间的计算
    // execute_read_txn_time    = ((double)exec_txn_time) / txn_commit * execute_sample_count;

    execute_txn_payload_time = execute_read_txn_time - execute_read_req_time;

    // 单位 us
    double execute_read_txn_average_time    = execute_read_txn_time * 1.00 / execute_sample_count / 1000;
    double execute_read_req_average_time    = execute_read_req_time * 1.00 / execute_sample_count / 1000;

    double fetch_unapplied_writes_info_average_time = fetch_unapplied_writes_info_time * 1.00 / execute_sample_count / 1000;
    double load_log_from_log_store_average_time     = load_log_from_log_store_time * 1.00 / execute_sample_count / 1000;
    double create_version_average_time              = create_version_time * 1.00 / execute_sample_count / 1000;

    double execute_txn_payload_average_time = execute_txn_payload_time * 1.00 / execute_sample_count / 1000;
    
    printf("execute read txn time breakdown: \n");
    printf("execute read txn average time:                    %.2lf \n", execute_read_txn_average_time);
    printf("execute read req average time & ratio:            %.2lf us  &  %.2lf\% \n", execute_read_req_average_time, execute_read_req_average_time / execute_read_txn_average_time * 100);
    printf("fetch unapplied writes info average time & ratio: %.2lf us  &  %.2lf\% \n", fetch_unapplied_writes_info_average_time, fetch_unapplied_writes_info_average_time / execute_read_txn_average_time * 100);
    printf("load log from log store average time & ratio:     %.2lf us  &  %.2lf\% \n", load_log_from_log_store_average_time, load_log_from_log_store_average_time / execute_read_txn_average_time * 100);
    printf("create version average time & ratio:              %.2lf us  &  %.2lf\% \n", create_version_average_time, create_version_average_time / execute_read_txn_average_time * 100);

    printf("execute txn payload average time & ratio:         %.2lf us  &  %.2lf\% \n", execute_txn_payload_average_time, execute_txn_payload_average_time / execute_read_txn_average_time * 100);


  #elif   AM_STRATEGY_TYPE == LOG_INDEX_VERSION_CHAIN_AM

    ClockTime replay_sample_count           = 0;

    ClockTime replay_total_time             = 0;

    ClockTime replay_idle_time              = 0;
    ClockTime replay_write_txn_time         = 0;

    ClockTime replay_load_analysis_log_time = 0;
    ClockTime replay_payload_time           = 0;
    ClockTime replay_other_time             = 0;

    ClockTime vl_maintain_log_chain_time    = 0;
    

    for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
    {
        replay_sample_count += replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_;

        replay_idle_time      += replay_thread_stats_[replay_thread_id]->replay_idle_time_;
        replay_write_txn_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_;

        replay_load_analysis_log_time += replay_thread_stats_[replay_thread_id]->load_analysis_log_time_;
        replay_payload_time += replay_thread_stats_[replay_thread_id]->replay_payload_time_;

        vl_maintain_log_chain_time    += replay_thread_stats_[replay_thread_id]->vl_maintain_log_chain_time_;

        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
        UtilFunc::Sort(replay_thread_stats_[replay_thread_id]->replay_idle_time_array_, 1, replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_);
    }

    replay_total_time = replay_idle_time + replay_write_txn_time;

    replay_other_time =   replay_write_txn_time 
                        - replay_load_analysis_log_time 
                        - replay_payload_time;

    printf("replay thread time breakdown: \n");
    printf("replay idle time ratio:          %.2lf\% \n", replay_idle_time * 1.00 / replay_total_time * 100);
    printf("replay write txn time ratio:     %.2lf\% \n", replay_write_txn_time * 1.00 / replay_total_time * 100);

    printf("replay write txn time breakdown: \n");

    //单位是 us
    double replay_write_txn_average_time         = replay_write_txn_time * 1.00 / replay_sample_count / 1000;
    double replay_load_analysis_log_average_time = replay_load_analysis_log_time * 1.00 / replay_sample_count / 1000;
    double replay_payload_average_time           = replay_payload_time * 1.00 / replay_sample_count / 1000;
    double replay_other_average_time             = replay_other_time * 1.00 / replay_sample_count / 1000;
    double vl_maintain_log_chain_average_time    = vl_maintain_log_chain_time * 1.00 / replay_sample_count / 1000;

    printf("replay write txn average time:       %.2lf \n", replay_write_txn_average_time);
    printf("log analysis time & ratio:           %.2lf us  &  %.2lf\% \n", replay_load_analysis_log_average_time, replay_load_analysis_log_average_time / replay_write_txn_average_time * 100);
    printf("replay payload time & ratio:         %.2lf us  &  %.2lf\% \n", replay_payload_average_time, replay_payload_average_time / replay_write_txn_average_time * 100);
    printf("replay other time & ratio:           %.2lf us  &  %.2lf\% \n", replay_other_average_time, replay_other_average_time / replay_write_txn_average_time * 100);
    printf("vl maintain log chain time & ratio:  %.2lf us  &  %.2lf\% \n", vl_maintain_log_chain_average_time, vl_maintain_log_chain_average_time / replay_write_txn_average_time * 100);

    printf("\n");


    printf("replay write txn time distribution: \n");
    // 输出回放写事务的时间分布
    for (uint64_t proportion = g_time_breakdown_distribution_proportion_start; proportion >= g_time_breakdown_distribution_proportion_end; proportion -= g_time_breakdown_distribution_proportion_gap)
    {
        ClockTime total_time = 0;

        for (ThreadID replay_thread_id = 0; replay_thread_id < g_replay_thread_num; replay_thread_id++)
        {
            uint64_t index = (uint64_t)(replay_thread_stats_[replay_thread_id]->time_break_down_sample_count_ * proportion * 1.00 / 100);
            total_time += replay_thread_stats_[replay_thread_id]->replay_write_txn_time_array_[index];
        }

        printf("proportion: %ld\%, replay time: %.2lf \n", proportion, total_time / g_replay_thread_num * 1.00 / 1000);
    }

    printf("\n");


    ClockTime execute_sample_count     = 0;

    ClockTime execute_read_txn_time    = 0;
    ClockTime execute_read_req_time    = 0;
    ClockTime execute_txn_payload_time = 0;
    // ClockTime oa_fetch_unapplied_writes_info_time = 0;
    // ClockTime oa_load_log_from_log_store_time     = 0;
    // ClockTime oa_create_version_time     = 0;


    for (ThreadID execute_thread_id = 0; execute_thread_id < g_txn_thread_num; execute_thread_id++)
    {
        execute_sample_count += execute_thread_stats_[execute_thread_id]->time_break_down_sample_count_;

        execute_read_txn_time += execute_thread_stats_[execute_thread_id]->execute_read_txn_;
        execute_read_req_time += execute_thread_stats_[execute_thread_id]->execute_read_request_;
    }

    // 使用非抽样数据作为事务的执行时间，是为了避免抽样中GET_CLOCK_TIME函数调用本身的用时影响事务执行时间的计算
    // execute_read_txn_time    = ((double)exec_txn_time) / txn_commit * execute_sample_count;

    execute_txn_payload_time = execute_read_txn_time - execute_read_req_time;

    // 单位 us
    double execute_read_txn_average_time    = execute_read_txn_time * 1.00 / execute_sample_count / 1000;
    double execute_read_req_average_time    = execute_read_req_time * 1.00 / execute_sample_count / 1000;
    double execute_txn_payload_average_time = execute_txn_payload_time * 1.00 / execute_sample_count / 1000;
    
    printf("execute read txn time breakdown: \n");
    printf("execute read txn average time:            %.2lf \n", execute_read_txn_average_time);
    printf("execute read req average time & ratio:    %.2lf us  &  %.2lf\% \n", execute_read_req_average_time, execute_read_req_average_time / execute_read_txn_average_time * 100);
    printf("execute txn payload average time & ratio: %.2lf us  &  %.2lf\% \n", execute_txn_payload_average_time, execute_txn_payload_average_time / execute_read_txn_average_time * 100);
    
    printf("\n\n\n");

    printf("reclaim write txn time breakdown: \n");

    ClockTime reclaim_sample_count    = 0;

    ClockTime reclaim_write_txn_time  = 0;
    ClockTime reclaim_idle_time       = 0;

    for (ThreadID reclaim_thread_id = 0; reclaim_thread_id < g_reclaim_thread_num; reclaim_thread_id++)
    {
        reclaim_sample_count   += reclaim_thread_stats_[reclaim_thread_id]->reclaim_write_txn_sample_count_;

        reclaim_write_txn_time += reclaim_thread_stats_[reclaim_thread_id]->reclaim_write_txn_time_;
        reclaim_idle_time      += reclaim_thread_stats_[reclaim_thread_id]->reclaim_idle_time_;
    }

    execute_txn_payload_time = execute_read_txn_time - execute_read_req_time;


    // 单位 us
    double reclaim_write_txn_average_time = reclaim_write_txn_time * 1.00 / execute_sample_count / 1000;
    double reclaim_idle_average_time      = reclaim_idle_time * 1.00 / execute_sample_count / 1000;
    double reclaim_average_time           = reclaim_write_txn_average_time + reclaim_idle_average_time;

    printf("reclaim write txn average ratio:  %.2lf\% \n", reclaim_write_txn_average_time / reclaim_average_time * 100);
    printf("reclaim idle average ratio:       %.2lf\% \n", reclaim_idle_average_time / reclaim_average_time * 100);
    

#endif


#endif



}
