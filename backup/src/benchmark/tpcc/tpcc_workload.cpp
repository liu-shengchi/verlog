#include "tpcc_workload.h"

#include "global.h"

#include "util_function.h"

#include "tpcc_schema.h"

#include "index.h"

#include "access_obj.h"

#include "txn_id.h"
#include "txn_context.h"

#include "statistic_manager.h"

#include <cmath>
#include <stdio.h>
#include <malloc.h>
#include <string.h>

#include <set>

using namespace std;


#if   WORKLOAD_TYPE == TPCC_W


/******** TPCCWorkload ********/

TPCCWorkload::TPCCWorkload()
{
    // txn_thread_num_     = g_txn_thread_num;
    // txn_num_per_thread_ = g_txns_num_per_thread;

    // txns_per_threads_ = new std::queue<Transaction*>[txn_thread_num_];
}


TPCCWorkload::~TPCCWorkload()
{

}


// Transaction* TPCCWorkload::GenTxn(ThreadID thread_id)
// {   
//     Transaction* txn  = nullptr;

//     if (thread_id < 0)
//     {
//         txn = new StockLevelInWH(TPCCTxnType::STOCK_LEVEL_IN_WH_TXN);
//     }
//     else
//     {
//         uint64_t     rand = UtilFunc::Rand(TXN_TOTAL_RATIO, 0);

//         if (rand < ORDERSTATUS_RATIO)
//             txn = new OrderStatus(TPCCTxnType::ORDER_STATUS_TXN);
//         else if (rand < STOCKLEVEL_RATIO)
//             txn = new StockLevel(TPCCTxnType::STOCK_LEVEL_TXN);
//     }
    
//     return txn;
// }


Transaction* TPCCWorkload::GenTxn()
{
    uint64_t     rand = UtilFunc::Rand(TXN_TOTAL_RATIO, 0);
    Transaction* txn  = nullptr;

    if (rand < ORDERSTATUS_RATIO)
        txn = new OrderStatus(TPCCTxnType::ORDER_STATUS_TXN);
    else if (rand < STOCKLEVEL_RATIO)
        txn = new StockLevel(TPCCTxnType::STOCK_LEVEL_TXN);

    return txn;
}


// bool TPCCWorkload::InsertNewTxn(ThreadID thread_id, Transaction* new_txn)
// {
//     waiting_queue_lock_[thread_id].GetSpinLock();

//     if (txns_per_threads_[thread_id].empty() || txns_per_threads_[thread_id].size() < g_waiting_txn_queue_capacity)
//     {
//         txns_per_threads_[thread_id].push(new_txn);
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return true;
//     }
//     else
//     {
//         waiting_queue_lock_[thread_id].ReleaseSpinLock();
//         return false;
//     }
// }




/************ OrderStatus ************/
OrderStatus::OrderStatus(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

OrderStatus::~OrderStatus()
{
}

void OrderStatus::GenInputData()
{
    ClientID client_id = txn_identifier_->GetClientId();

    w_id_ = UtilFunc::URand(1, g_warehouse_num, client_id);
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, client_id);
	
    int x = UtilFunc::URand(1, 100, client_id);
    if (x <= TPCC_BY_LAST_NAME_RATIO)
    {
        by_last_name_ = true;
        UtilFunc::Lastname(UtilFunc::NURand(255, 0, 999, client_id), c_last_name_);
    }
    else
    {
        by_last_name_ = false;
        c_id_ = UtilFunc::NURand(1023, 1, g_cust_per_dist, client_id);
    }
}

void OrderStatus::GenInputData(uint64_t ware_id)
{
    ClientID client_id = txn_identifier_->GetClientId();

    w_id_ = ware_id;
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, client_id);
	
    int x = UtilFunc::URand(1, 100, client_id);
    if (x <= TPCC_BY_LAST_NAME_RATIO)
    {
        by_last_name_ = true;
        UtilFunc::Lastname(UtilFunc::NURand(255, 0, 999, client_id), c_last_name_);
    }
    else
    {
        by_last_name_ = false;
        c_id_ = UtilFunc::NURand(1023, 1, g_cust_per_dist, client_id);
        // c_id_ = UtilFunc::URand(1, g_cust_per_dist, thread_id);
    }
}

RC OrderStatus::RunTxn()
{
    RC rc = RC::RC_OK;

    Index*       index     = nullptr;
    IndexKey     index_key = 0;
    void*        index_attr[5] = {nullptr};
    PartitionKey part_key  = 0;
    void*        part_attr[5] = {nullptr};

    AccessObj* read_ao = nullptr;

    char       tuple_data[MAX_TUPLE_SIZE];
    ColumnData column_data = nullptr;

    //customer
    uint64_t c_balance   = 0;
    char     c_first[16] = {'0'};
    char     c_middle[2] = {'0'};
    char     c_last[16]  = {'0'};
    //order
    uint64_t o_id         = 0;
    uint64_t o_entry_d    = 0;
    uint64_t o_carrier_id = 0;
    //order_line
    uint64_t ol_i_id[16]        = {0};
    uint64_t ol_supply_w_id[16] = {0};
    uint64_t ol_quantity[16]    = {0};
    double   ol_amount[16]      = {0};
    uint64_t ol_delivery_d[16]  = {0};

#if     TIME_BREAKDOWN_ANALYTIC
    ClockTime execute_read_req_begin_time_;
    ClockTime execute_read_req_end_time_;
#endif


    int count = 0;
    for (uint64_t i = 0; i < 80; i++)
    {
        count++;
    }


    if (by_last_name_)
    {
        /*************
            EXEC SQL SELECT count(c_id) INTO :namecnt 
            FROM customer
            WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
            
            EXEC SQL DECLARE c_name CURSOR FOR
            SELECT c_balance, c_first, c_middle, c_id
            FROM customer
            WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
            ORDER BY c_first;
            
            EXEC SQL OPEN c_name;
            if (namecnt%2) namecnt++; / / Locate midpoint customer
            for (n=0; n<namecnt/ 2; n++)
            {
                EXEC SQL FETCH c_name
                INTO :c_balance, :c_first, :c_middle, :c_id; 
            }
            EXEC SQL CLOSE c_name;
         *************/
    }
    else
    {
        /**********************************************
            EXEC SQL SELECT c_balance, c_first, c_middle, c_last
            INTO :c_balance, :c_first, :c_middle, :c_last 
            FROM customer
            WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
        ***********************************************/

    #if     TIME_BREAKDOWN_ANALYTIC
        if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
        {
            GET_CLOCK_TIME(execute_read_req_begin_time_);
        }
    #endif
    
        index = g_schema->GetIndex(C_PK_INDEX, w_id_-1);
        index_attr[0] = &w_id_;
        index_attr[1] = &d_id_;
        index_attr[2] = &c_id_;
        index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);

        rc = index->IndexRead(index_key, read_ao);

        txn_context_->AccessTuple(read_ao, TPCCTableType::CUSTOMER_T, w_id_-1, index_key, tuple_data);


    #if     TIME_BREAKDOWN_ANALYTIC
        if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
        {
            GET_CLOCK_TIME(execute_read_req_end_time_);
    
            g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
        }
    #endif


        //c_balance
        g_schema->GetColumnValue(CUSTOMER_T, C_BALANCE, tuple_data, column_data);
        c_balance = *(uint64_t*)column_data;

        //c_first, c_middle, c_last
        g_schema->GetColumnValue(CUSTOMER_T, C_FIRST, tuple_data, column_data);
        memcpy(c_first, column_data, 16);

        g_schema->GetColumnValue(CUSTOMER_T, C_MIDDLE, tuple_data, column_data);
        memcpy(c_middle, column_data, 2);

        g_schema->GetColumnValue(CUSTOMER_T, C_LAST, tuple_data, column_data);
        memcpy(c_last, column_data, 16);

    }


    /*********************
        EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
        INTO :o_id, :o_carrier_id, :entdate
        FROM orders
        ORDER BY o_id DESC;
     *********************/

#if     TIME_BREAKDOWN_ANALYTIC
     if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
     {
         GET_CLOCK_TIME(execute_read_req_begin_time_);
     }
 #endif

    index = g_schema->GetIndex(O_CUST_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_attr[2] = &c_id_;
    uint64_t max_o_id = pow(2, O_ID_BITS) - 1;
    index_attr[3] = &max_o_id;
    index_key = g_schema->GetIndexKey(O_CUST_INDEX, index_attr);

    part_attr[0] = &w_id_;
    part_attr[1] = &d_id_;
    part_attr[2] = &c_id_;
    part_key = g_schema->GetPartKey(O_CUST_INDEX, part_attr);
    
    rc = index->IndexMaxRead(index_key, read_ao, part_key, txn_context_->GetReadTS());

    if (rc == RC_NULL)
    {
        printf("OrderStatus error: There must be at least one order per customer\n");
        goto abort_txn;
    }
    // else
    // {
    //     printf("success!\n");
    // }


#if    AM_STRATEGY_TYPE == ON_DEMAND_APPLY_WRITES_AM
    txn_context_->AccessTuple(read_ao, TPCCTableType::ORDER_T, w_id_-1, read_ao->pk_, tuple_data);
#elif  AM_STRATEGY_TYPE == QUERY_FRESH_AM
    txn_context_->AccessTuple(read_ao, TPCCTableType::ORDER_T, w_id_-1, read_ao->oid_, tuple_data);
#else
    txn_context_->AccessTuple(read_ao, TPCCTableType::ORDER_T, w_id_-1, index_key, tuple_data);
#endif
    

#if     TIME_BREAKDOWN_ANALYTIC
    if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
    {
        GET_CLOCK_TIME(execute_read_req_end_time_);

        g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
    }
#endif


    //o_id
    g_schema->GetColumnValue(ORDER_T, O_ID, tuple_data, column_data);
    o_id = *(uint64_t*)column_data;
    //o_entry_d
    g_schema->GetColumnValue(ORDER_T, O_ENTRY_D, tuple_data, column_data);
    o_entry_d = *(uint64_t*)column_data;
    //o_carrier_id
    g_schema->GetColumnValue(ORDER_T, O_CARRIER_ID, tuple_data, column_data);
    o_carrier_id = *(uint64_t*)column_data;


    /*******************
        EXEC SQL DECLARE c_line CURSOR FOR
        SELECT ol_i_id, ol_supply_w_id, ol_quantity,
               ol_amount, ol_delivery_d
        FROM  order_line
        WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
        
        EXEC SQL OPEN c_line;
        EXEC SQL WHENEVER NOT FOUND CONTINUE;

        i=0;
        while (sql_notfound(FALSE))
        {
            i++;
            EXEC SQL FETCH c_line
            INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], 
                 :ol_amount[i], :ol_delivery_d[i];
        }     
     *******************/


    index = g_schema->GetIndex(OL_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_attr[2] = &o_id;

    for (uint64_t ol_number = 1; ol_number <= 15; ol_number++)
    {
        index_attr[3] = &ol_number;
        index_key = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);


    #if     TIME_BREAKDOWN_ANALYTIC
        if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
        {
            GET_CLOCK_TIME(execute_read_req_begin_time_);
        }
    #endif

        rc = index->IndexRead(index_key, read_ao);

        if (rc == RC_NULL)
            break;

        txn_context_->AccessTuple(read_ao, TPCCTableType::ORDER_LINE_T,w_id_-1, index_key, tuple_data);


    #if     TIME_BREAKDOWN_ANALYTIC
        if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
        {
            GET_CLOCK_TIME(execute_read_req_end_time_);
    
            g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
        }
    #endif

        //ol_i_id
        g_schema->GetColumnValue(ORDER_LINE_T, OL_I_ID, tuple_data, column_data);
        ol_i_id[ol_number] = *(uint64_t*)column_data;
        //ol_supply_w_id
        g_schema->GetColumnValue(ORDER_LINE_T, OL_SUPPLY_W_ID, tuple_data, column_data);
        ol_supply_w_id[ol_number] = *(uint64_t*)column_data;
        //ol_quantity
        g_schema->GetColumnValue(ORDER_LINE_T, OL_QUANTITY, tuple_data, column_data);
        ol_quantity[ol_number] = *(uint64_t*)column_data;
        //ol_amount
        g_schema->GetColumnValue(ORDER_LINE_T, OL_AMOUNT, tuple_data, column_data);
        ol_amount[ol_number] = *(double*)column_data;
        //ol_delivery_d
        g_schema->GetColumnValue(ORDER_LINE_T, OL_DELIVERY_D, tuple_data, column_data);
        ol_delivery_d[ol_number] = *(uint64_t*)column_data;
    }


commit_txn:
    // printf("ORDER_STATUS{w_id: %ld, d_id: %ld, c_id: %ld, o_id: %ld}\n", w_id_, d_id_, c_id_, o_id);
    // printf("ORDER_STATUS Commit!\n");
    return RC_COMMIT;

abort_txn:
    printf("ORDER_STATUS Error!\n");
    return RC_ABORT;
}



/*************** StockLevel *****************/
StockLevel::StockLevel(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

StockLevel::~StockLevel()
{
}


void StockLevel::GenInputData()
{
    ClientID client_id = txn_identifier_->GetClientId();

    w_id_ = UtilFunc::URand(1, g_warehouse_num, client_id);
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, client_id);
	stock_min_threshold_ = 10;

}


void StockLevel::GenInputData(uint64_t ware_id)
{
    ClientID client_id = txn_identifier_->GetClientId();
    
    w_id_ = ware_id;
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, client_id);
	stock_min_threshold_ = UtilFunc::URand(10, 20, client_id);
}


RC StockLevel::RunTxn()
{
    RC rc = RC::RC_OK;
    
    Index*   dist_index    = nullptr;
    Index*   ol_index      = nullptr;
    Index*   stock_index   = nullptr;
    void*    index_attr[5] = {nullptr};
    IndexKey index_key     = 0;

    AccessObj* read_ao     = nullptr;
    char       tuple_data[MAX_TUPLE_SIZE];
    ColumnData column_data = nullptr;

    uint64_t o_id = 0;
    set<uint64_t> s_i_id_set;
    uint64_t stock_count = 0;
    

    /*******************
        EXEC SQL SELECT d_next_o_id INTO :o_id
        FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id; 
    ********************/

#if     TIME_BREAKDOWN_ANALYTIC
    ClockTime execute_read_req_begin_time_;
    ClockTime execute_read_req_end_time_;

    if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
    {
        GET_CLOCK_TIME(execute_read_req_begin_time_);
    }
#endif


    dist_index = g_schema->GetIndex(D_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_key     = g_schema->GetIndexKey(D_PK_INDEX, index_attr);

    dist_index->IndexRead(index_key, read_ao);

    txn_context_->AccessTuple(read_ao, TPCCTableType::DISTRICT_T, w_id_-1, index_key, tuple_data);


#if     TIME_BREAKDOWN_ANALYTIC
    if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
    {
        GET_CLOCK_TIME(execute_read_req_end_time_);

        g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
    }
#endif


    //d_next_o_id
    g_schema->GetColumnValue(DISTRICT_T, D_NEXT_O_ID, tuple_data, column_data);
    o_id = *(uint64_t*)column_data;


    /*****************         
        EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
        FROM order_line, stock
        WHERE ol_w_id=:w_id AND
              ol_d_id=:d_id AND ol_o_id<:o_id AND
              ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
              s_i_id=ol_i_id AND s_quantity < :threshold;
     ****************/

    uint64_t ol_supply_w_id = 0;
    uint64_t ol_i_id        = 0;
    uint64_t s_quantity     = 0;

    ol_index    = g_schema->GetIndex(OL_PK_INDEX, w_id_-1);

    for (uint64_t ol_o_id = o_id - g_stock_level_scan_order_num; ol_o_id < o_id; ol_o_id++)
    {
        
        for (uint64_t ol_number = 1; ol_number <= 15; ol_number++)
        {
        #if     TIME_BREAKDOWN_ANALYTIC
            if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
            {
                GET_CLOCK_TIME(execute_read_req_begin_time_);
            }
        #endif

            index_attr[0] = &w_id_;
            index_attr[1] = &d_id_;
            index_attr[2] = &ol_o_id;
            index_attr[3] = &ol_number;
            index_key     = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);

            //获取w_id下的order_line
            rc = ol_index->IndexRead(index_key, read_ao);
            if (rc == RC_NULL)
            {   
                break;
            }

            txn_context_->AccessTuple(read_ao, TPCCTableType::ORDER_LINE_T, w_id_-1, index_key, tuple_data);


        #if     TIME_BREAKDOWN_ANALYTIC
            if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
            {
                GET_CLOCK_TIME(execute_read_req_end_time_);
        
                g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
            }
        #endif

            //ol_supply_w_id
            g_schema->GetColumnValue(ORDER_LINE_T, OL_SUPPLY_W_ID, tuple_data, column_data);
            ol_supply_w_id = *(uint64_t*)column_data;

            //ol_i_id
            g_schema->GetColumnValue(ORDER_LINE_T, OL_I_ID, tuple_data, column_data);
            ol_i_id = *(uint64_t*)column_data;



        #if     TIME_BREAKDOWN_ANALYTIC
            if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
            {
                GET_CLOCK_TIME(execute_read_req_begin_time_);
            }
        #endif

            /* TODO: 存在bug待修改 */
            // index_attr[0] = &ol_supply_w_id;
            index_attr[0] = &w_id_;

            index_attr[1] = &ol_i_id;
            index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);

            // stock_index = g_schema->GetIndex(S_PK_INDEX, ol_supply_w_id-1);
            stock_index = g_schema->GetIndex(S_PK_INDEX, w_id_ - 1);


            // index_attr[0] = &w_id_;
            // index_attr[1] = &ol_i_id;
            // index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);

            // stock_index = g_schema->GetIndex(S_PK_INDEX, w_id_-1);

            rc = stock_index->IndexRead(index_key, read_ao);

            if (rc == RC_NULL)
            {
                printf("stock获取异常!\n");
                goto abort_txn;
            }
            
            // txn_context_->AccessTuple(read_ao, TPCCTableType::STOCK_T, ol_supply_w_id-1, index_key, tuple_data);
            txn_context_->AccessTuple(read_ao, TPCCTableType::STOCK_T, w_id_-1, index_key, tuple_data);

        #if     TIME_BREAKDOWN_ANALYTIC
            if (txn_context_->IsTimeBreakdownSample() && g_system_state == TESTING_STATE)
            {
                GET_CLOCK_TIME(execute_read_req_end_time_);
        
                g_statistic_manager->TimeBreakdownStatc(txn_context_->GetExecuteThreadID(), TXN_THREAD_T, EXECUTE_READ_REQUEST_TIME_T, execute_read_req_end_time_ - execute_read_req_begin_time_);
            }
        #endif

            //s_quantity
            g_schema->GetColumnValue(STOCK_T, S_QUANTITY, tuple_data, column_data);
            s_quantity = *(uint64_t*)column_data;
            
            
            auto insert_result = s_i_id_set.insert(ol_i_id);
            //插入成功（之前没有遍历到该s_i_id），且s_quantity小于阈值
            if (insert_result.second && s_quantity < stock_min_threshold_)
                stock_count++;
        }
    }

commit_txn:

    // printf("STOCK_LEVEL{w_id: %ld, d_id: %ld, threshold: %ld, low_stock: %ld}\n", w_id_, d_id_, stock_min_threshold_, stock_count);
    // printf("STOCK_LEVEL Commit!\n");
    // printf("\n");
    return RC_COMMIT;

abort_txn:
    printf("STOCK_LEVEL Error!\n");
    return RC_ABORT;
}





/*************** StockLevelInWH *****************/
StockLevelInWH::StockLevelInWH(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

StockLevelInWH::~StockLevelInWH()
{
}


void StockLevelInWH::GenInputData()
{
    ClientID client_id = txn_identifier_->GetClientId();

    w_id_          = UtilFunc::URand(1, g_warehouse_num, client_id);
    start_i_id_    = UtilFunc::URand(1, g_item_num - g_tpcc_scan_num + 1, client_id);
    end_i_id_      = start_i_id_ + g_tpcc_scan_num - 1;
    
	stock_min_threshold_ = UtilFunc::URand(10, 20, client_id);

}


void StockLevelInWH::GenInputData(uint64_t ware_id)
{
    ClientID client_id = txn_identifier_->GetClientId();
    
    w_id_          = ware_id;
    start_i_id_    = UtilFunc::URand(1, g_item_num - g_tpcc_scan_num + 1, client_id);
    end_i_id_      = start_i_id_ + g_tpcc_scan_num - 1;
    
	stock_min_threshold_ = UtilFunc::URand(10, 20, client_id);
}


RC StockLevelInWH::RunTxn()
{
    RC rc = RC::RC_OK;
    
    Index*   dist_index    = nullptr;
    Index*   ol_index      = nullptr;
    Index*   stock_index   = nullptr;
    void*    index_attr[5] = {nullptr};
    IndexKey index_key     = 0;

    AccessObj* read_ao     = nullptr;
    char       tuple_data[MAX_TUPLE_SIZE];
    ColumnData column_data = nullptr;

    uint64_t s_quantity = 0;
    set<uint64_t> s_i_id_set;
    uint64_t stock_count = 0;
    

    for (uint64_t i_id = start_i_id_; i_id <= end_i_id_; i_id++)
    {
        /* TODO: 存在bug待修改 */
        index_attr[0] = &w_id_;
        index_attr[1] = &i_id;
        index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);

        stock_index = g_schema->GetIndex(S_PK_INDEX, w_id_-1);

        // index_attr[0] = &w_id_;
        // index_attr[1] = &ol_i_id;
        // index_key     = g_schema->GetIndexKey(S_PK_INDEX, index_attr);

        // stock_index = g_schema->GetIndex(S_PK_INDEX, w_id_-1);

        rc = stock_index->IndexRead(index_key, read_ao);

        if (rc == RC_NULL)
        {
            printf("stock获取异常!\n");
            goto abort_txn;
        }

        txn_context_->AccessTuple(read_ao, TPCCTableType::STOCK_T, w_id_ - 1, index_key, tuple_data);

        //s_quantity
        g_schema->GetColumnValue(STOCK_T, S_QUANTITY, tuple_data, column_data);
        s_quantity = *(uint64_t*)column_data;
        
        
        auto insert_result = s_i_id_set.insert(i_id);
        //插入成功（之前没有遍历到该s_i_id），且s_quantity小于阈值
        if (insert_result.second && s_quantity < stock_min_threshold_)
            stock_count++;
    }
    

commit_txn:

    // printf("STOCK_LEVEL{w_id: %ld, d_id: %ld, threshold: %ld, low_stock: %ld}\n", w_id_, d_id_, stock_min_threshold_, stock_count);
    // printf("STOCK_LEVEL Commit!\n");
    // printf("\n");
    return RC_COMMIT;

abort_txn:
    printf("STOCK_LEVEL Error!\n");
    return RC_ABORT;
}


#endif
