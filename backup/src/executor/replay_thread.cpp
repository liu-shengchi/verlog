#include "replay_thread.h"

#include "global.h"

#include "log_manager.h"
#include "log_buffer.h"

#include "log_replay_context.h"
#include "log_strategy.h"

#include "statistic_manager.h"


ReplayThread::ReplayThread(ThreadID thread_id, ThreadID replay_thread_id, ProcID processor_id)
{
    thread_type_  = DBThreadType::REPLAY_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = processor_id;

    replay_thread_id_   = replay_thread_id;

    replay_log_buf_num_  = 0;
    replay_thread_index_ = 0;
    for (int i = 0; i < g_log_buffer_num; i++)
    {
        replay_log_buf_[i]     = nullptr;
        log_replay_context_[i] = nullptr;
    }

    AllocLogBufToReplayer();
}

ReplayThread::~ReplayThread()
{
}


void ReplayThread::AllocLogBufToReplayer()
{
    if (g_replay_thread_num % g_log_buffer_num == 0)
    {
        replay_log_buf_num_    = 1;
        replay_log_buf_[0]     = g_log_manager->GetLogBuffer(replay_thread_id_ % g_log_buffer_num);
        log_replay_context_[0] = new LogReplayContext();
        log_replay_context_[0]->InitContext(this, g_am_strategy_, g_log_strategy_, replay_log_buf_[0], replay_thread_id_ / g_log_buffer_num);
    }
    else if (g_log_buffer_num % g_replay_thread_num == 0)
    {

    }
    else
    {
        int alloced_thread   = 0;
        int unalloced_thread = g_replay_thread_num;

        while (alloced_thread < g_replay_thread_num)
        {
            int max_comm_div = unalloced_thread;
            for (; max_comm_div >= 1; max_comm_div--)
            {
                if (g_log_buffer_num % max_comm_div == 0)
                // max_comm_div <= g_log_buffer_num
                    break;
            }

            if (alloced_thread + max_comm_div <= replay_thread_id_)
            {
                alloced_thread   += max_comm_div;
                unalloced_thread -= max_comm_div;
                
                replay_thread_index_++;
                continue;
            }

            replay_log_buf_num_ = g_log_buffer_num / max_comm_div;
            for (int i = 0; i < replay_log_buf_num_; i++)
            {
                replay_log_buf_[i]     = g_log_manager->GetLogBuffer(i * max_comm_div + replay_thread_id_ - alloced_thread);
                log_replay_context_[i] = new LogReplayContext();
                log_replay_context_[i]->InitContext(this, g_am_strategy_, g_log_strategy_, replay_log_buf_[i], replay_thread_index_);
            }
            break;
        }
    }
}



void ReplayThread::Run()
{
#if THREAD_BIND_CORE
        SetAffinity();
#endif


#if    TIME_BREAKDOWN_ANALYTIC

    uint64_t time_breakdown_frequency_count = 0;

#endif

#if    MICRO_STATISTIC_ANALYTIC

    uint64_t micro_statistic_frequency_count = 0;

#endif


#if    TIME_BREAKDOWN_ANALYTIC
    ClockTime replay_write_txn_begin_time_ = 0;
    ClockTime replay_write_txn_end_time_   = 0;
    
    ClockTime load_analysis_begin_time_ = 0;
    ClockTime load_analysis_end_time_   = 0;

    ClockTime replay_payload_begin_time_ = 0;
    ClockTime replay_payload_end_time_   = 0;

    ClockTime replay_idle_begin_time_ = 0;
    ClockTime replay_idle_end_time_   = 0;
#endif




    while (g_system_state != SystemState::FINISH_STATE)
    {

        for (int log_buf_index = 0; log_buf_index < replay_log_buf_num_; log_buf_index++)
        {
            LogBuffer*        log_buf = replay_log_buf_[log_buf_index];
            LogReplayContext* log_replay_context = log_replay_context_[log_buf_index];

            log_buf->replaying_[replay_thread_index_] = true;
            COMPILER_BARRIER


            if (!log_replay_context->GetReplayTxnLog())
            {
                COMPILER_BARRIER
                log_buf->replaying_[replay_thread_index_] = false;

                continue;
            }

        #if    TIME_BREAKDOWN_ANALYTIC
            time_breakdown_frequency_count++;

            if ((time_breakdown_frequency_count % g_time_breakdown_sample_frequency == 0) && g_system_state == TESTING_STATE)
            {
                log_replay_context->is_time_breakdown_sample_ = true;
            }
            else
            {
                if (log_replay_context->is_time_breakdown_sample_ == true)
                {
                    GET_CLOCK_TIME(replay_idle_end_time_);
                    g_statistic_manager->TimeBreakdownStatc(replay_thread_id_, REPLAY_THREAD_T, REPLAY_IDLE_TIME_T, replay_idle_end_time_ - replay_idle_begin_time_);
                }
                
                log_replay_context->is_time_breakdown_sample_ = false;
            }

            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(replay_write_txn_begin_time_);
                load_analysis_begin_time_ = replay_write_txn_begin_time_;
            }

        #endif

        #if    TIME_BREAKDOWN_ANALYTIC && USING_REPLAY_LOAD_ANALYSIS_LOG_TIME_T
            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(load_analysis_begin_time_);
            }
        #endif


            log_replay_context->DeconstructTxnLog();


        #if    TIME_BREAKDOWN_ANALYTIC && USING_REPLAY_LOAD_ANALYSIS_LOG_TIME_T
            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(load_analysis_end_time_);

                g_statistic_manager->TimeBreakdownStatc(replay_thread_id_, REPLAY_THREAD_T, REPLAY_LOAD_ANALYSIS_LOG_TIME_T, load_analysis_end_time_ - load_analysis_begin_time_);
            }
        #endif


        #if    TIME_BREAKDOWN_ANALYTIC && USING_REPLAY_PAYLOAD_TIME_T
            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(replay_payload_begin_time_);
            }
        #endif





        #if    MICRO_STATISTIC_ANALYTIC
            micro_statistic_frequency_count++;

            if ((micro_statistic_frequency_count % g_micro_sample_frequency == 0) && g_system_state == TESTING_STATE)
            {
                log_replay_context->is_micro_statistic_sample_ = true;
            }

            if (log_replay_context->IsMicroStatisticSample())
            {
                g_statistic_manager->MicroParameterStatc(replay_thread_id_, REPLAY_THREAD_T, REPLAY_WRITE_TXN_COUNT_T, 1);
            }
        #endif


            log_replay_context->ReplayTxnLog();
        

        #if    TIME_BREAKDOWN_ANALYTIC && USING_REPLAY_PAYLOAD_TIME_T
            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(replay_payload_end_time_);

                g_statistic_manager->TimeBreakdownStatc(replay_thread_id_, REPLAY_THREAD_T, REPLAY_PAYLOAD_TIME_T, replay_payload_end_time_ - replay_payload_begin_time_);
            }
        #endif


            log_replay_context->FinishReplayTxn();


            COMPILER_BARRIER
            log_buf->replayed_commit_ts_per_replayer_[replay_thread_index_] = log_replay_context->replayed_commit_ts_;
            log_buf->replayed_lsn_per_replayer_[replay_thread_index_] = log_replay_context->replayed_lsn_;

            COMPILER_BARRIER
            log_buf->replaying_[replay_thread_index_] = false;

            COMPILER_BARRIER

            Timestamp min_commit_ts    = log_replay_context->replayed_commit_ts_;
            LogLSN    min_replayed_lsn = log_replay_context->replayed_lsn_;
            
            for (uint64_t i = 0; i < log_buf->replayer_num_; i++)
            {
                if (log_buf->replaying_[i])
                {
                    Timestamp replaying_ts = log_buf->replaying_commit_ts_[i];
                    if (min_commit_ts > replaying_ts && replaying_ts !=0)
                        min_commit_ts = replaying_ts - 1;

                    if (min_replayed_lsn > log_buf->replayed_lsn_per_replayer_[i])
                        min_replayed_lsn = log_buf->replayed_lsn_per_replayer_[i];
                }
            }
            
            log_buf->replayed_commit_ts_ = min_commit_ts;
            log_buf->replayed_lsn_       = min_replayed_lsn;




        #if    TIME_BREAKDOWN_ANALYTIC
            if (log_replay_context->IsTimeBreakdownSample())
            {
                GET_CLOCK_TIME(replay_write_txn_end_time_);

                g_statistic_manager->TimeBreakdownStatc(replay_thread_id_, REPLAY_THREAD_T, REPLAY_WRITE_TXN_TIME_T, replay_write_txn_end_time_ - replay_write_txn_begin_time_);

                GET_CLOCK_TIME(replay_idle_begin_time_);
            }
        #endif


        #if    MICRO_STATISTIC_ANALYTIC
            if (log_replay_context->IsMicroStatisticSample())
            {
                log_replay_context->is_micro_statistic_sample_ = false;
            }
        #endif

        }
    }
}
