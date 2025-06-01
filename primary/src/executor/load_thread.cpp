#include "load_thread.h"

#include "global.h"

#include "util_function.h"

#include "tpcc_schema.h"
#include "tpcc_config.h"
#include "tpcc_util.h"

#include "ycsb_schema.h"
#include "ycsb_config.h"

#include "smallbank_schema.h"
#include "smallbank_config.h"

#include "index.h"
#include "tuple.h"
#include "access_entry.h"

#include "mvto.h"

#include "log_manager.h"
#include "log_strategy.h"
#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"

#include "log_buffer.h"

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

using namespace std;



LoadThread::LoadThread(ThreadID thread_id, ProcID processor_id, uint64_t min_w_id, uint64_t max_w_id)
{
    thread_type_  = DBThreadType::LOAD_THREAD_T;
    thread_id_    = thread_id;
    processor_id_ = processor_id;
    
#if LOAD_DATA_LOG
    log_buffer_   = g_log_manager->GetLogBuffer(thread_id % g_log_buffer_num);
    log_entry_    = (char*)malloc(MAX_TXN_LOG_SIZE);
#endif


#if WORKLOAD_TYPE == TPCC_W

    min_w_id_     = min_w_id;
    max_w_id_     = max_w_id;

#elif WORKLOAD_TYPE == YCSB_W
    min_shard_ = min_w_id;
    max_shard_ = max_w_id;

#elif WORKLOAD_TYPE == SMALLBANK_W

    min_shard_ = min_w_id;
    max_shard_ = max_w_id;

    // printf("undefined load process in smallbank!\n");
    // exit(0);

#endif

}

LoadThread::~LoadThread()
{
#if LOAD_DATA_LOG
    free(log_entry_);
#endif
}

void LoadThread::Run()
{
    printf("thread %d start load data!\n", thread_id_);

#if   WORKLOAD_TYPE == TPCC_W

    #if   STORAGE_STRATEGY_TYPE == SHARD_BY_WAREHOUSE_SS

        for (ShardID shard_id = min_w_id_ - 1; shard_id <= max_w_id_ - 1; shard_id++)
        {
            for (IndexID index_id = 0; index_id < TPCC_INDEX_NUM; index_id++)
            {
                if (index_id == I_PK_INDEX)
                    continue;
                
                g_schema->CreateIndex(index_id, shard_id);
            }
        }
        

        if (thread_id_ == 0)
        {
            for (ShardID shard_id = 0; shard_id < g_warehouse_num; shard_id++)
            {   
                g_schema->CreateIndex(I_PK_INDEX, shard_id);
            }
        }

    #elif STORAGE_STRATEGY_TYPE == NO_SHARD_SS

        //在初始化Schema阶段已经完成索引的创建

    #endif


    if (thread_id_ == 0)
    {
        LoadItem();
    }

    for (uint64_t w_id = min_w_id_; w_id <= max_w_id_; w_id++)
    {
        LoadWarehouse(w_id);
        LoadDistrict(w_id);
        LoadStock(w_id);

        for (uint64_t d_id = 1; d_id <= g_dist_per_ware; d_id++)
        {
            LoadCustomer(w_id, d_id);
            LoadOrder(w_id, d_id);

            for (uint64_t c_id = 1; c_id <= g_cust_per_dist; c_id++)
            {
                LoadHistory(w_id, d_id, c_id);
            }
        }
    }


#elif WORKLOAD_TYPE == YCSB_W
    LoadYCSBTable();


#elif WORKLOAD_TYPE == SMALLBANK_W

    for (uint64_t shard_id = min_shard_; shard_id <= max_shard_; shard_id++)
    {
        LoadAccountTable(shard_id);
        LoadSavingTable(shard_id);
        LoadCheckingTable(shard_id);
    }

    // printf("undefined smallbank load process!\n");
    // exit(0);

#endif



    printf("thread %d finish load data!\n", thread_id_);
    pthread_exit(NULL); 
}



#if   WORKLOAD_TYPE == TPCC_W

void LoadThread::LoadItem()
{
    Index* index = g_schema->GetIndex(I_PK_INDEX, 0);

    void* index_attr[1];

    for (uint64_t item_id = 1; item_id <= g_item_num; item_id++)
    {
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(ITEM_T));

        uint64_t i_im_id;
        char     i_name[24];
        uint64_t i_price;
        char     i_data[50];

        i_im_id = UtilFunc::URand(1L, 10000L, thread_id_);
        TPCCUtilFunc::MakeAlphaString(14, 24, i_name, thread_id_);
        i_price = UtilFunc::URand(1, 100, 0);
        
        uint64_t len = TPCCUtilFunc::MakeAlphaString(26, 50, i_data, thread_id_);
        if (UtilFunc::Rand(100, thread_id_) < 10)
        {
            uint64_t idx = UtilFunc::URand(0, len - 9, thread_id_);
            strcpy(&i_data[idx], "original");
        }

        g_schema->SetColumnValue(ITEM_T, I_ID, tuple->tuple_data_, (ColumnData)&item_id);
        g_schema->SetColumnValue(ITEM_T, I_IM_ID, tuple->tuple_data_, (ColumnData)&i_im_id);
        g_schema->SetColumnValue(ITEM_T, I_NAME, tuple->tuple_data_, (ColumnData)i_data);
        g_schema->SetColumnValue(ITEM_T, I_PRICE, tuple->tuple_data_, (ColumnData)&i_price);
        g_schema->SetColumnValue(ITEM_T, I_DATA, tuple->tuple_data_, (ColumnData)i_data);


#if LOAD_DATA_LOG
        //写入日志
        {
            LogLSN   log_lsn  = 0;
            uint64_t log_size = 0;


        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif
            
            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作
            // index_and_opts[0].index_id_  = I_PK_INDEX;
            // index_and_opts[0].index_opt_ = INSERT_AT;

            log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            tuple, INSERT_AT, 0, ITEM_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();
            
            log_lsn = log_buffer_->AtomicFetchLSN(log_size);

            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);
            
            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
            
            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

        index_attr[0] = &item_id;
        IndexKey index_key = g_schema->GetIndexKey(I_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
}


void LoadThread::LoadWarehouse(uint64_t  w_id)
{
    Tuple* tuple = new Tuple(g_schema->GetTupleSize(WAREHOUSE_T));

    g_schema->SetColumnValue(WAREHOUSE_T, W_ID, tuple->tuple_data_, (ColumnData)&w_id);

    char     w_name[10];
    TPCCUtilFunc::MakeAlphaString(6, 10, w_name, thread_id_);
    g_schema->SetColumnValue(WAREHOUSE_T, W_NAME, tuple->tuple_data_, (ColumnData)w_name);

    char     w_street[20];
    TPCCUtilFunc::MakeAlphaString(10, 20, w_street, thread_id_);
    g_schema->SetColumnValue(WAREHOUSE_T, W_STREET_1, tuple->tuple_data_, (ColumnData)w_street);
    TPCCUtilFunc::MakeAlphaString(10, 20, w_street, thread_id_);
    g_schema->SetColumnValue(WAREHOUSE_T, W_STREET_2, tuple->tuple_data_, (ColumnData)w_street);
    TPCCUtilFunc::MakeAlphaString(10, 20, w_street, thread_id_);
	g_schema->SetColumnValue(WAREHOUSE_T, W_CITY, tuple->tuple_data_, (ColumnData)w_street);

    char w_state[2];
    TPCCUtilFunc::MakeAlphaString(2, 2, w_state, thread_id_);
    g_schema->SetColumnValue(WAREHOUSE_T, W_STATE, tuple->tuple_data_, (ColumnData)w_state);

    char w_zip[9];
    TPCCUtilFunc::MakeAlphaString(9, 9, w_zip, thread_id_);
    g_schema->SetColumnValue(WAREHOUSE_T, W_ZIP, tuple->tuple_data_, (ColumnData)w_zip);

    double w_tax = (double)UtilFunc::URand(0L, 200L, thread_id_)/1000.0;
    double w_ytd = 300000.00;
    g_schema->SetColumnValue(WAREHOUSE_T, W_TAX, tuple->tuple_data_, (ColumnData)&w_tax);
    g_schema->SetColumnValue(WAREHOUSE_T, W_YTD, tuple->tuple_data_, (ColumnData)&w_ytd);

#if LOAD_DATA_LOG
    //写入日志
    {
        uint64_t log_size = 0;
        LogLSN   log_lsn  = 0;

    #if   LOG_STRATEGY_TYPE == SERIAL_LOG
        SerialTupleMeta log_tuple_meta;
    #elif LOG_STRATEGY_TYPE == TAURUS_LOG
        TaurusTupleMeta log_tuple_meta;
    #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
        LogIndexTupleMeta log_tuple_meta;
    #endif

        uint64_t    index_and_opt_cnt = 0;
        IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
        //IndexAndOpt只记录二级索引变更操作
        // index_and_opts[0].index_id_  = W_PK_INDEX;
        // index_and_opts[0].index_opt_ = INSERT_AT;

        log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            tuple, INSERT_AT, w_id-1, WAREHOUSE_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

        COMPILER_BARRIER

        log_buffer_->log_buf_spinlock_.GetSpinLock();

        log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
        
        COMPILER_BARRIER
        log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);
        
        COMPILER_BARRIER
        log_buffer_->unpersistented_lsn_ += log_size;
        log_buffer_->unreplicated_lsn_   += log_size;

        log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

        COMPILER_BARRIER
        g_log_strategy_->CommitTuple(
                                &log_tuple_meta,
                                log_buffer_->log_buffer_id_,
                                log_lsn);
        
        COMPILER_BARRIER
        tuple->CopyLogTupleMeta(&log_tuple_meta);
    }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

    Index*   index     = g_schema->GetIndex(W_PK_INDEX, w_id-1);
    IndexKey index_key = w_id;
    index->IndexInsert(index_key, tuple);
}


void LoadThread::LoadDistrict(uint64_t  w_id)
{
    Index* index = g_schema->GetIndex(D_PK_INDEX, w_id-1);
    
    void* index_attr[2];
    index_attr[0] = &w_id;

    for (uint64_t  d_id = 1; d_id <= g_dist_per_ware; d_id++)
    {
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(DISTRICT_T));

        g_schema->SetColumnValue(DISTRICT_T, D_W_ID, tuple->tuple_data_, (ColumnData)&w_id);
        g_schema->SetColumnValue(DISTRICT_T, D_ID, tuple->tuple_data_, (ColumnData)&d_id);
        
        char d_name[10];
        TPCCUtilFunc::MakeAlphaString(6, 10, d_name, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_NAME, tuple->tuple_data_, (ColumnData)d_name);

        char d_street[20];
        TPCCUtilFunc::MakeAlphaString(10, 20, d_street, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_STREET_1, tuple->tuple_data_, (ColumnData)d_street);
        TPCCUtilFunc::MakeAlphaString(10, 20, d_street, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_STREET_2, tuple->tuple_data_, (ColumnData)d_street);
        TPCCUtilFunc::MakeAlphaString(10, 20, d_street, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_CITY, tuple->tuple_data_, (ColumnData)d_street);
        
        char d_state[2];
        TPCCUtilFunc::MakeAlphaString(2, 2, d_state, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_STATE, tuple->tuple_data_, (ColumnData)d_state);

        char d_zip[9];
        TPCCUtilFunc::MakeNumberString(9, 9, d_zip, thread_id_);
        g_schema->SetColumnValue(DISTRICT_T, D_ZIP, tuple->tuple_data_, (ColumnData)d_zip);

        double d_tax = (double)UtilFunc::URand(0L, 200L, thread_id_)/1000.0;
    	double d_ytd = 30000.00;
        g_schema->SetColumnValue(DISTRICT_T, D_TAX, tuple->tuple_data_, (ColumnData)&d_tax);
        g_schema->SetColumnValue(DISTRICT_T, D_YTD, tuple->tuple_data_, (ColumnData)&d_ytd);

        uint64_t d_next_o_id = 3001;
        g_schema->SetColumnValue(DISTRICT_T, D_NEXT_O_ID, tuple->tuple_data_, (ColumnData)&d_next_o_id);

#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif
            
            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作
            // index_and_opts[0].index_id_  = D_PK_INDEX;
            // index_and_opts[0].index_opt_ = INSERT_AT;

            log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_,
                                            tuple, INSERT_AT, w_id-1, DISTRICT_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();

            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);

            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);
            

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
        
            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }

#endif

#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

        index_attr[1] = &d_id;
        IndexKey index_key = g_schema->GetIndexKey(D_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
    
}


void LoadThread::LoadStock(uint64_t  w_id)
{
    Index* index = g_schema->GetIndex(S_PK_INDEX, w_id-1);
    
    IndexKey index_key = 0;
    void* index_attr[2];
    index_attr[0] = &w_id;

    for (uint64_t  s_i_id = 1; s_i_id <= g_item_num; s_i_id++)
    {
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(STOCK_T));

        g_schema->SetColumnValue(STOCK_T, S_I_ID, tuple->tuple_data_, (ColumnData)&s_i_id);
        g_schema->SetColumnValue(STOCK_T, S_W_ID, tuple->tuple_data_, (ColumnData)&w_id);

        uint64_t s_quantity = UtilFunc::URand(10, 100, thread_id_);
        g_schema->SetColumnValue(STOCK_T, S_QUANTITY, tuple->tuple_data_, (ColumnData)&s_quantity);

        uint64_t s_remote_cnt = 0;
        g_schema->SetColumnValue(STOCK_T,S_REMOTE_CNT, tuple->tuple_data_, (ColumnData)&s_remote_cnt);
        
        char s_dist[25];
        for (uint64_t i = 0; i < 10; i++)
        {
			TPCCUtilFunc::MakeAlphaString(24, 24, s_dist, thread_id_);
			g_schema->SetColumnValue(STOCK_T, (ColumnID)(S_DIST_01 + i), tuple->tuple_data_, (ColumnData)&s_dist);
		}

        uint64_t s_ytd = 0;
        g_schema->SetColumnValue(STOCK_T, S_YTD, tuple->tuple_data_, (ColumnData)&s_ytd);

        uint64_t s_order_cnt = 0;
        g_schema->SetColumnValue(STOCK_T, S_ORDER_CNT, tuple->tuple_data_, (ColumnData)&s_order_cnt);

        char s_data[50];
        uint64_t len = TPCCUtilFunc::MakeAlphaString(26, 50, s_data, thread_id_);
        if (UtilFunc::Rand(100, thread_id_) < 10)
        {
            uint64_t idx = UtilFunc::URand(0, len - 9, thread_id_);
            strcpy(&s_data[idx], "original");
        }
        g_schema->SetColumnValue(STOCK_T, S_DATA, tuple->tuple_data_, (ColumnData)&s_data);


#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作
            // index_and_opts[0].index_id_  = S_PK_INDEX;
            // index_and_opts[0].index_opt_ = INSERT_AT;
        
            log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            tuple, INSERT_AT, w_id-1, STOCK_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();
            
            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
            
            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
        
            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }

#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif
        index_attr[1] = &s_i_id;
        index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
}


void LoadThread::LoadCustomer(uint64_t  w_id, uint64_t d_id)
{
    Index* c_pk_index = g_schema->GetIndex(C_PK_INDEX, w_id-1);
    Index* c_last_index = g_schema->GetIndex(C_LAST_INDEX, w_id-1);

    IndexKey index_key = 0;

    void* c_pk_index_attr[3];
    c_pk_index_attr[0] = &w_id;
    c_pk_index_attr[1] = &d_id;

    void* c_last_index_attr[1];


    for (uint64_t  c_id = 1; c_id <= g_cust_per_dist; c_id++)
    {
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(CUSTOMER_T));

        g_schema->SetColumnValue(CUSTOMER_T, C_W_ID, tuple->tuple_data_, (ColumnData)&w_id);
        g_schema->SetColumnValue(CUSTOMER_T, C_D_ID, tuple->tuple_data_, (ColumnData)&d_id);
        g_schema->SetColumnValue(CUSTOMER_T, C_ID, tuple->tuple_data_, (ColumnData)&c_id);

        char c_last[16];
        memset(c_last, '\0', 16);
        if (c_id <= 1000)
            TPCCUtilFunc::Lastname(c_id - 1, c_last);
        else
            TPCCUtilFunc::Lastname(TPCCUtilFunc::NURand(255, 0, 999, thread_id_), c_last);
        g_schema->SetColumnValue(CUSTOMER_T, C_LAST, tuple->tuple_data_, (ColumnData)c_last);

        char c_middle[] = "OE";
        g_schema->SetColumnValue(CUSTOMER_T, C_MIDDLE, tuple->tuple_data_, (ColumnData)c_middle);

        char c_first[16];
        TPCCUtilFunc::MakeAlphaString(8, sizeof(c_first), c_first, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_FIRST, tuple->tuple_data_, (ColumnData)c_first);

        char street[20];
        TPCCUtilFunc::MakeAlphaString(10, 20, street, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_STREET_1, tuple->tuple_data_, (ColumnData)street);
        TPCCUtilFunc::MakeAlphaString(10, 20, street, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_STREET_2, tuple->tuple_data_, (ColumnData)street);
        TPCCUtilFunc::MakeAlphaString(10, 20, street, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_CITY, tuple->tuple_data_, (ColumnData)street);

        char c_state[2];
        TPCCUtilFunc::MakeAlphaString(2, 2, c_state, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_STATE, tuple->tuple_data_, (ColumnData)c_state);

        char c_zip[9];
        TPCCUtilFunc::MakeNumberString(9, 9, c_zip, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_ZIP, tuple->tuple_data_, (ColumnData)c_zip);

        char c_phone[16];
        TPCCUtilFunc::MakeNumberString(16, 16, c_phone, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_PHONE, tuple->tuple_data_, (ColumnData)c_phone);

        uint64_t c_since = 0;
        g_schema->SetColumnValue(CUSTOMER_T, C_SINCE, tuple->tuple_data_, (ColumnData)&c_since);

        if (UtilFunc::Rand(10, thread_id_) != 0)
        {
            //90% ratio
            char c_credit[] = "GC";
            g_schema->SetColumnValue(CUSTOMER_T, C_CREDIT, tuple->tuple_data_, (ColumnData)&c_credit);
        }
        else
        {
            //10% ratio
            char c_credit[] = "BC";
            g_schema->SetColumnValue(CUSTOMER_T, C_CREDIT, tuple->tuple_data_, (ColumnData)&c_credit);
        }
        
        uint64_t c_credit_lim = 50000;
        g_schema->SetColumnValue(CUSTOMER_T, C_CREDIT_LIM, tuple->tuple_data_, (ColumnData)&c_credit_lim);

        double c_discount = (double)UtilFunc::URand(0L, 500L, thread_id_)/1000.0;
        g_schema->SetColumnValue(CUSTOMER_T, C_DISCOUNT, tuple->tuple_data_, (ColumnData)&c_discount);

        double c_balance     = -10.0;
        g_schema->SetColumnValue(CUSTOMER_T, C_BALANCE, tuple->tuple_data_, (ColumnData)&c_balance);

        double c_ytd_payment = 10.0;
        g_schema->SetColumnValue(CUSTOMER_T, C_YTD_PAYMENT, tuple->tuple_data_, (ColumnData)&c_ytd_payment);

        uint64_t c_payment_cnt = 1;
        g_schema->SetColumnValue(CUSTOMER_T, C_PAYMENT_CNT, tuple->tuple_data_, (ColumnData)&c_payment_cnt);

        char c_data[500];
        TPCCUtilFunc::MakeAlphaString(300, 500, c_data, thread_id_);
        g_schema->SetColumnValue(CUSTOMER_T, C_DATA, tuple->tuple_data_, (ColumnData)&c_data);

        uint64_t c_delivery_cnt = 0;
        g_schema->SetColumnValue(CUSTOMER_T, C_DELIVERY_CNT, tuple->tuple_data_, (ColumnData)&c_delivery_cnt);

#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 1;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作
            // index_and_opts[0].index_id_  = C_PK_INDEX;
            // index_and_opts[0].index_opt_ = INSERT_AT;
            index_and_opts[0].index_id_  = C_LAST_INDEX;
            index_and_opts[0].index_opt_ = INSERT_AT;
            
            log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_,
                                            tuple, INSERT_AT, w_id-1, CUSTOMER_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

            COMPILER_BARRIER
            
            log_buffer_->log_buf_spinlock_.GetSpinLock();
            
            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);

            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
            
            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }


#endif

#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif
        
        c_pk_index_attr[2] = &c_id;
        index_key = g_schema->GetIndexKey(C_PK_INDEX, c_pk_index_attr);
        c_pk_index->IndexInsert(index_key, tuple);
        
        c_last_index_attr[0] = c_last;
        index_key = g_schema->GetIndexKey(C_LAST_INDEX, c_last_index_attr);
        c_last_index->IndexInsert(index_key, tuple);
    }
}


void LoadThread::LoadOrder(uint64_t w_id, uint64_t d_id)
{
    uint64_t perm_c_id[g_cust_per_dist]; 
	// init_permutation(perm, wid); /* initialize permutation of customer numbers */
    for (uint64_t i = 0; i < g_cust_per_dist; i++)
        perm_c_id[i] = i + 1;
    
    for (uint64_t i = 0; i < g_cust_per_dist - 1; i++)
    {
        uint64_t j = UtilFunc::URand(i+1, g_cust_per_dist-1, thread_id_);
        uint64_t tmp = perm_c_id[i];
        perm_c_id[i] = perm_c_id[j];
        perm_c_id[j] = tmp;
    }
    
    Index* o_pk_index  = g_schema->GetIndex(ORDER_T, w_id-1);
    Index* ol_pk_index = g_schema->GetIndex(ORDER_LINE_T, w_id-1);
    Index* no_pk_index = g_schema->GetIndex(NEW_ORDER_T, w_id-1);

    IndexKey index_key;
    void* index_attr[4];
    index_attr[0] = &w_id;
    index_attr[1] = &d_id;


	for (uint64_t o_id = 1; o_id <= g_cust_per_dist; o_id++)
    {
        Tuple* o_tuple = new Tuple(g_schema->GetTupleSize(ORDER_T));
        
        uint64_t o_c_id = perm_c_id[o_id - 1];

        g_schema->SetColumnValue(ORDER_T, O_W_ID, o_tuple->tuple_data_, (ColumnData)&w_id);
        g_schema->SetColumnValue(ORDER_T, O_D_ID, o_tuple->tuple_data_, (ColumnData)&d_id);
        g_schema->SetColumnValue(ORDER_T, O_C_ID, o_tuple->tuple_data_, (ColumnData)&o_c_id);
        g_schema->SetColumnValue(ORDER_T, O_ID, o_tuple->tuple_data_, (ColumnData)&o_id);

        uint64_t o_entry_d = 2023;
        g_schema->SetColumnValue(ORDER_T, O_ENTRY_D, o_tuple->tuple_data_, (ColumnData)&o_entry_d);

        uint64_t o_carrier_id;
        if (o_id <= 2100)
            o_carrier_id = UtilFunc::URand(1, 10, thread_id_);
        else
            o_carrier_id = 0;
        g_schema->SetColumnValue(ORDER_T, O_CARRIER_ID, o_tuple->tuple_data_, (ColumnData)&o_carrier_id);

        uint64_t o_ol_cnt = UtilFunc::URand(5, 15, thread_id_);
        g_schema->SetColumnValue(ORDER_T, O_OL_CNT, o_tuple->tuple_data_, (ColumnData)&o_ol_cnt);

        uint64_t o_all_local = 1;
        g_schema->SetColumnValue(ORDER_T, O_ALL_LOCAL, o_tuple->tuple_data_, (ColumnData)&o_all_local);
		
#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 1;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作
            // index_and_opts[0].index_id_  = O_PK_INDEX;
            // index_and_opts[0].index_opt_ = INSERT_AT;
            index_and_opts[0].index_id_  = O_CUST_INDEX;
            index_and_opts[0].index_opt_ = INSERT_AT;
        
            log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            o_tuple, INSERT_AT, w_id-1, ORDER_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);
            
            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();
            
            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
            
            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
            
            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            o_tuple->CopyLogTupleMeta(&log_tuple_meta);
        }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(o_tuple);
        o_tuple = (Tuple*)mv_meta;
#endif

        index_attr[2] = &o_id;
        index_key = g_schema->GetIndexKey(O_PK_INDEX, index_attr);
        o_pk_index->IndexInsert(index_key, o_tuple);


		// ORDER-LINE	
		for (uint64_t ol_number = 1; ol_number <= o_ol_cnt; ol_number++)
        {
            Tuple* ol_tuple = new Tuple(g_schema->GetTupleSize(ORDER_LINE_T));
            
            g_schema->SetColumnValue(ORDER_LINE_T, OL_W_ID, ol_tuple->tuple_data_, (ColumnData)&w_id);
            g_schema->SetColumnValue(ORDER_LINE_T, OL_D_ID, ol_tuple->tuple_data_, (ColumnData)&d_id);
            g_schema->SetColumnValue(ORDER_LINE_T, OL_O_ID, ol_tuple->tuple_data_, (ColumnData)&o_id);
            g_schema->SetColumnValue(ORDER_LINE_T, OL_NUMBER, ol_tuple->tuple_data_, (ColumnData)&ol_number);

            uint64_t ol_i_id = UtilFunc::URand(1, g_item_num, thread_id_);
            g_schema->SetColumnValue(ORDER_LINE_T, OL_I_ID, ol_tuple->tuple_data_, (ColumnData)&ol_i_id);

            g_schema->SetColumnValue(ORDER_LINE_T, OL_SUPPLY_W_ID, ol_tuple->tuple_data_, (ColumnData)&w_id);

            uint64_t ol_delivery_d;
            double   ol_amount;

			if (o_id <= 2100) {
                ol_delivery_d = o_entry_d;
                ol_amount = 0;
			} else {
                ol_delivery_d = 0;
                ol_amount     = (double)UtilFunc::URand(1, 999999, thread_id_) / 100;
			}

            g_schema->SetColumnValue(ORDER_LINE_T, OL_DELIVERY_D, ol_tuple->tuple_data_, (ColumnData)&ol_delivery_d);
            g_schema->SetColumnValue(ORDER_LINE_T, OL_AMOUNT, ol_tuple->tuple_data_, (ColumnData)&ol_amount);

            uint64_t ol_quantity = 5;
            g_schema->SetColumnValue(ORDER_LINE_T, OL_QUANTITY, ol_tuple->tuple_data_, (ColumnData)&ol_quantity);

			char ol_dist_info[24];
	        TPCCUtilFunc::MakeAlphaString(24, 24, ol_dist_info, thread_id_);
			g_schema->SetColumnValue(ORDER_LINE_T, OL_DIST_INFO, ol_tuple->tuple_data_, (ColumnData)&ol_dist_info);


#if LOAD_DATA_LOG
            //写入日志
            {
                uint64_t log_size = 0;
                LogLSN   log_lsn  = 0;

            #if   LOG_STRATEGY_TYPE == SERIAL_LOG
                SerialTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == TAURUS_LOG
                TaurusTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
                LogIndexTupleMeta log_tuple_meta;
            #endif

                uint64_t    index_and_opt_cnt = 0;
                IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
                //IndexAndOpt只记录二级索引变更操作
                // index_and_opts[0].index_id_  = OL_PK_INDEX;
                // index_and_opts[0].index_opt_ = INSERT_AT;

                log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_,
                                            ol_tuple, INSERT_AT, w_id-1, ORDER_LINE_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

                COMPILER_BARRIER

                log_buffer_->log_buf_spinlock_.GetSpinLock();
                
                log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
                
                COMPILER_BARRIER
                log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

                COMPILER_BARRIER
                log_buffer_->unpersistented_lsn_ += log_size;
                log_buffer_->unreplicated_lsn_   += log_size;

                log_buffer_->log_buf_spinlock_.ReleaseSpinLock();
            
                COMPILER_BARRIER
                g_log_strategy_->CommitTuple(
                                        &log_tuple_meta,
                                        log_buffer_->log_buffer_id_,
                                        log_lsn);
                
                COMPILER_BARRIER
                ol_tuple->CopyLogTupleMeta(&log_tuple_meta);
            }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
            MvtoVersionMeta* mv_meta = new MvtoVersionMeta(ol_tuple);
            ol_tuple = (Tuple*)mv_meta;
#endif

            index_attr[3] = &ol_number;
            index_key = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);
            ol_pk_index->IndexInsert(index_key, ol_tuple);
		}

		// NEW ORDER
		if (o_id > 2100)
        {
			Tuple* no_tuple = new Tuple(g_schema->GetTupleSize(NEW_ORDER_T));

            g_schema->SetColumnValue(NEW_ORDER_T, NO_W_ID, no_tuple->tuple_data_, (ColumnData)&w_id);
            g_schema->SetColumnValue(NEW_ORDER_T, NO_D_ID, no_tuple->tuple_data_, (ColumnData)&d_id);
            g_schema->SetColumnValue(NEW_ORDER_T, NO_O_ID, no_tuple->tuple_data_, (ColumnData)&o_id);

#if LOAD_DATA_LOG
            //写入日志
            {
                uint64_t log_size = 0;
                LogLSN   log_lsn  = 0;

            #if   LOG_STRATEGY_TYPE == SERIAL_LOG
                SerialTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == TAURUS_LOG
                TaurusTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
                LogIndexTupleMeta log_tuple_meta;
            #endif

                uint64_t    index_and_opt_cnt = 0;
                IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
                //IndexAndOpt只记录二级索引变更操作
                // index_and_opts[0].index_id_  = NO_PK_INDEX;
                // index_and_opts[0].index_opt_ = INSERT_AT;

                log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            no_tuple, INSERT_AT, w_id-1, NEW_ORDER_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

                COMPILER_BARRIER

                log_buffer_->log_buf_spinlock_.GetSpinLock();

                log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
                
                COMPILER_BARRIER
                log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

                COMPILER_BARRIER
                log_buffer_->unpersistented_lsn_ += log_size;
                log_buffer_->unreplicated_lsn_   += log_size;

                log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

                COMPILER_BARRIER
                g_log_strategy_->CommitTuple(
                                        &log_tuple_meta,
                                        log_buffer_->log_buffer_id_,
                                        log_lsn);
                
                COMPILER_BARRIER
                no_tuple->CopyLogTupleMeta(&log_tuple_meta);
            }

#endif

#if CC_STRATEGY_TYPE == MVTO_CC
            MvtoVersionMeta* mv_meta = new MvtoVersionMeta(no_tuple);
            no_tuple = (Tuple*)mv_meta;
#endif

            index_key = g_schema->GetIndexKey(NO_PK_INDEX, index_attr);
            no_pk_index->IndexInsert(index_key, no_tuple);
		}
	}    
}


void LoadThread::LoadHistory(uint64_t  w_id, uint64_t d_id, uint64_t c_id)
{
    Tuple* tuple = new Tuple(g_schema->GetTupleSize(HISTORY_T));

    uint64_t h_id = g_schema->FetchIncID(HISTORY_T);
    g_schema->SetColumnValue(HISTORY_T, H_ID, tuple->tuple_data_, (ColumnData)&h_id);
    g_schema->SetColumnValue(HISTORY_T, H_W_ID, tuple->tuple_data_, (ColumnData)&w_id);
    g_schema->SetColumnValue(HISTORY_T, H_D_ID, tuple->tuple_data_, (ColumnData)&d_id);
    g_schema->SetColumnValue(HISTORY_T, H_C_ID, tuple->tuple_data_, (ColumnData)&c_id);
    g_schema->SetColumnValue(HISTORY_T, H_C_W_ID, tuple->tuple_data_, (ColumnData)&w_id);
    g_schema->SetColumnValue(HISTORY_T, H_C_D_ID, tuple->tuple_data_, (ColumnData)&d_id);

    uint64_t h_date = 0;
    g_schema->SetColumnValue(HISTORY_T, H_DATE, tuple->tuple_data_, (ColumnData)&h_date);

    double h_amount = 10.0;
    g_schema->SetColumnValue(HISTORY_T, H_AMOUNT, tuple->tuple_data_, (ColumnData)&h_amount);

	char h_data[24];
	TPCCUtilFunc::MakeAlphaString(12, 24, h_data, thread_id_);
    g_schema->SetColumnValue(HISTORY_T, H_DATA, tuple->tuple_data_, (ColumnData)h_data);

#if LOAD_DATA_LOG
    //写入日志
    {
        uint64_t log_size = 0;
        LogLSN   log_lsn  = 0;

    #if   LOG_STRATEGY_TYPE == SERIAL_LOG
        SerialTupleMeta log_tuple_meta;
    #elif LOG_STRATEGY_TYPE == TAURUS_LOG
        TaurusTupleMeta log_tuple_meta;
    #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
        LogIndexTupleMeta log_tuple_meta;
    #endif

        uint64_t    index_and_opt_cnt = 0;
        IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
        //IndexAndOpt只记录二级索引变更操作
        // index_and_opts[0].index_id_  = H_PK_INDEX;
        // index_and_opts[0].index_opt_ = INSERT_AT;

        log_size = g_log_strategy_->ConstructLoadLog(
                                            log_entry_, 
                                            log_buffer_->log_buffer_id_, 
                                            tuple, INSERT_AT, w_id-1, HISTORY_T,
                                            index_and_opt_cnt, index_and_opts, 
                                            0,
                                            &log_tuple_meta);

        COMPILER_BARRIER

        log_buffer_->log_buf_spinlock_.GetSpinLock();

        log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
        
        COMPILER_BARRIER
        log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

        COMPILER_BARRIER
        log_buffer_->unpersistented_lsn_ += log_size;
        log_buffer_->unreplicated_lsn_   += log_size;

        log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

        COMPILER_BARRIER
        g_log_strategy_->CommitTuple(
                                &log_tuple_meta,
                                log_buffer_->log_buffer_id_,
                                log_lsn);
        
        COMPILER_BARRIER
        tuple->CopyLogTupleMeta(&log_tuple_meta);
    }

#endif

#if CC_STRATEGY_TYPE == MVTO_CC
    MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
    tuple = (Tuple*)mv_meta;
#endif

    Index* index = g_schema->GetIndex(H_PK_INDEX, w_id-1);

    IndexKey index_key;
    void* index_attr[1];
    index_attr[0] = &h_id;
    index_key = g_schema->GetIndexKey(H_PK_INDEX, index_attr);
    
    index->IndexInsert(index_key, tuple);
}


#elif WORKLOAD_TYPE == YCSB_W

void LoadThread::LoadYCSBTable(){

    for (ShardID shard_id = min_shard_ - 1; shard_id < max_shard_; shard_id++)
    {
        Index* index = g_schema->GetIndex(YCSB_INDEX, shard_id);

        void *index_attr[1];

        LogLSN   log_lsn  = 0;
        uint64_t log_size = 0;

        for (uint64_t record_id = 1; record_id <= g_ycsb_record_num_per_shard; record_id++)
        {
            Tuple* tuple = new Tuple(g_schema->GetTupleSize(YCSB_TABLE));

            g_schema->SetColumnValue(YCSB_TABLE, YCSB_KEY, tuple->tuple_data_, (ColumnData)&record_id);
            
            char FD0[g_ycsb_field_size];
            UtilFunc::MakeAlphaString(0, sizeof(FD0), FD0, thread_id_);
            g_schema->SetColumnValue(YCSB_TABLE, F0, tuple->tuple_data_, (ColumnData)FD0);
            // char FD1[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD1), FD1, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F1, tuple->tuple_data_, (ColumnData)FD1);
            // char FD2[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD2), FD2, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F2, tuple->tuple_data_, (ColumnData)FD2);
            // char FD3[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD3), FD3, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F3, tuple->tuple_data_, (ColumnData)FD3);
            // char FD4[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD4), FD4, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F4, tuple->tuple_data_, (ColumnData)FD4);
            // char FD5[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD5), FD5, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F5, tuple->tuple_data_, (ColumnData)FD5);
            // char FD6[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD6), FD6, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F6, tuple->tuple_data_, (ColumnData)FD6);
            // char FD7[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD7), FD7, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F7, tuple->tuple_data_, (ColumnData)FD7);
            // char FD8[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD8), FD8, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F8, tuple->tuple_data_, (ColumnData)FD8);
            // char FD9[YCSB_FIELD_SIZE];
            // UtilFunc::MakeAlphaString(0, sizeof(FD9), FD9, thread_id_);
            // g_schema->SetColumnValue(YCSB_TABLE, F9, tuple->tuple_data_, (ColumnData)FD9);

    #if LOAD_DATA_LOG
            //写入日志
            {
                uint64_t log_size = 0;
                LogLSN   log_lsn  = 0;

            #if   LOG_STRATEGY_TYPE == SERIAL_LOG
                SerialTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == TAURUS_LOG
                TaurusTupleMeta log_tuple_meta;
            #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
                LogIndexTupleMeta log_tuple_meta;
            #endif

                uint64_t    index_and_opt_cnt = 0;
                IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
                //IndexAndOpt只记录二级索引变更操作
                // index_and_opts[0].index_id_  = H_PK_INDEX;
                // index_and_opts[0].index_opt_ = INSERT_AT;

                log_size = g_log_strategy_->ConstructLoadLog(
                                                    log_entry_, 
                                                    log_buffer_->log_buffer_id_, 
                                                    tuple, INSERT_AT, shard_id, YCSB_TABLE,
                                                    index_and_opt_cnt, index_and_opts, 
                                                    0,
                                                    &log_tuple_meta);

                COMPILER_BARRIER

                log_buffer_->log_buf_spinlock_.GetSpinLock();

                log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
                
                COMPILER_BARRIER
                log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

                COMPILER_BARRIER
                log_buffer_->unpersistented_lsn_ += log_size;
                log_buffer_->unreplicated_lsn_   += log_size;

                log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

                COMPILER_BARRIER
                g_log_strategy_->CommitTuple(
                                        &log_tuple_meta,
                                        log_buffer_->log_buffer_id_,
                                        log_lsn);
                
                COMPILER_BARRIER
                tuple->CopyLogTupleMeta(&log_tuple_meta);
            }
    #endif


    #if CC_STRATEGY_TYPE == MVTO_CC
            MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
            tuple = (Tuple*)mv_meta;
    #endif
            index_attr[0] = &record_id;
            IndexKey index_key = g_schema->GetIndexKey(YCSB_INDEX, index_attr);
            
            index->IndexInsert(index_key, tuple);


            uint64_t count = 0;
            for (uint64_t i = 0; i < g_load_think_time; i++)
            {
                count++;
            }
            
        }
    }
    
}


#elif WORKLOAD_TYPE == SMALLBANK_W

void LoadThread::LoadAccountTable(ShardID shard_id)
{
    g_schema->CreateIndex(A_PK_INDEX, shard_id);

    Index* index = g_schema->GetIndex(A_PK_INDEX, shard_id);

    void *index_attr[1];

    LogLSN   log_lsn  = 0;
    uint64_t log_size = 0;

    for (uint64_t record_id = 1; record_id <= g_smallbank_account_num_per_shard; record_id++)
    {
        //初始化元组
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(SMALLBANKTableType::ACCOUNT_T));

        g_schema->SetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_NAME, tuple->tuple_data_, (ColumnData)&record_id);
        g_schema->SetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple->tuple_data_, (ColumnData)&record_id);
        

#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作

            log_size = g_log_strategy_->ConstructLoadLog(
                                                log_entry_, 
                                                log_buffer_->log_buffer_id_, 
                                                tuple, INSERT_AT, shard_id, SMALLBANKTableType::ACCOUNT_T,
                                                index_and_opt_cnt, index_and_opts, 
                                                0,
                                                &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();

            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
            
            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

        // 插入索引
        index_attr[0] = &record_id;
        IndexKey index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
}


void LoadThread::LoadSavingTable(ShardID shard_id)
{
    g_schema->CreateIndex(S_PK_INDEX, shard_id);

    Index* index = g_schema->GetIndex(S_PK_INDEX, shard_id);

    void *index_attr[1];

    LogLSN   log_lsn  = 0;
    uint64_t log_size = 0;

    for (uint64_t record_id = 1; record_id <= g_smallbank_account_num_per_shard; record_id++)
    {
        //初始化元组
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(SMALLBANKTableType::SAVING_T));

        g_schema->SetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_CUSTOMER_ID, tuple->tuple_data_, (ColumnData)&record_id);
        
        double s_balance = UtilFunc::URand(100, 1000, thread_id_) + UtilFunc::URand(1, 1000, thread_id_) / 1000.0;
        g_schema->SetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple->tuple_data_, (ColumnData)&s_balance);


#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作

            log_size = g_log_strategy_->ConstructLoadLog(
                                                log_entry_, 
                                                log_buffer_->log_buffer_id_, 
                                                tuple, INSERT_AT, shard_id, SMALLBANKTableType::SAVING_T,
                                                index_and_opt_cnt, index_and_opts, 
                                                0,
                                                &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();

            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
            
            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

        // 插入索引
        index_attr[0] = &record_id;
        IndexKey index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
}


void LoadThread::LoadCheckingTable(ShardID shard_id)
{
    g_schema->CreateIndex(C_PK_INDEX, shard_id);
    Index* index = g_schema->GetIndex(C_PK_INDEX, shard_id);

    void *index_attr[1];

    LogLSN   log_lsn  = 0;
    uint64_t log_size = 0;

    for (uint64_t record_id = 1; record_id <= g_smallbank_account_num_per_shard; record_id++)
    {
        //初始化元组
        Tuple* tuple = new Tuple(g_schema->GetTupleSize(SMALLBANKTableType::CHECKING_T));

        g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_CUSTOMER_ID, tuple->tuple_data_, (ColumnData)&record_id);
        
        double c_balance = UtilFunc::URand(100, 1000, thread_id_) + UtilFunc::URand(1, 1000, thread_id_) / 1000.0;
        g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple->tuple_data_, (ColumnData)&c_balance);


#if LOAD_DATA_LOG
        //写入日志
        {
            uint64_t log_size = 0;
            LogLSN   log_lsn  = 0;

        #if   LOG_STRATEGY_TYPE == SERIAL_LOG
            SerialTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == TAURUS_LOG
            TaurusTupleMeta log_tuple_meta;
        #elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
            LogIndexTupleMeta log_tuple_meta;
        #endif

            uint64_t    index_and_opt_cnt = 0;
            IndexAndOpt index_and_opts[g_max_sec_indx_per_table];
            //IndexAndOpt只记录二级索引变更操作

            log_size = g_log_strategy_->ConstructLoadLog(
                                                log_entry_, 
                                                log_buffer_->log_buffer_id_, 
                                                tuple, INSERT_AT, shard_id, SMALLBANKTableType::CHECKING_T,
                                                index_and_opt_cnt, index_and_opts, 
                                                0,
                                                &log_tuple_meta);

            COMPILER_BARRIER

            log_buffer_->log_buf_spinlock_.GetSpinLock();

            log_lsn  = log_buffer_->AtomicFetchLSN(log_size);
            
            COMPILER_BARRIER
            log_buffer_->SynLogToBuffer(log_lsn, log_entry_, log_size);

            COMPILER_BARRIER
            log_buffer_->unpersistented_lsn_ += log_size;
            log_buffer_->unreplicated_lsn_   += log_size;

            log_buffer_->log_buf_spinlock_.ReleaseSpinLock();

            COMPILER_BARRIER
            g_log_strategy_->CommitTuple(
                                    &log_tuple_meta,
                                    log_buffer_->log_buffer_id_,
                                    log_lsn);
            
            COMPILER_BARRIER
            tuple->CopyLogTupleMeta(&log_tuple_meta);
        }
#endif


#if CC_STRATEGY_TYPE == MVTO_CC
        MvtoVersionMeta* mv_meta = new MvtoVersionMeta(tuple);
        tuple = (Tuple*)mv_meta;
#endif

        // 插入索引
        index_attr[0] = &record_id;
        IndexKey index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
        index->IndexInsert(index_key, tuple);
    }
}


#endif
