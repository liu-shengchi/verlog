#include "tpcc_workload.h"
#include "tpcc_schema.h"
#include "tpcc_config.h"
#include "tpcc_util.h"

#include "global.h"

#include "util_function.h"

#include "index.h"
#include "tuple.h"
#include "access_entry.h"

#include "txn_id.h"
#include "txn_context.h"

#include <stdio.h>
#include <malloc.h>
#include <string.h>
#include <vector>

using namespace std;


#if   WORKLOAD_TYPE == TPCC_W


/******** TPCCWorkload ********/

TPCCWorkload::TPCCWorkload()
{
    txn_thread_num_     = g_txn_thread_num;
    txn_num_per_thread_ = g_txns_num_per_thread;

    txns_per_threads_ = new std::queue<Transaction*>[txn_thread_num_];

    
    for (uint64_t  i = 0; i < g_warehouse_num; i++)
    {
        deliverying[i] = false;
        for (uint64_t  j = 0; j < g_dist_per_ware; j++)
        {
            //注意，根据LoadThread实现，
            //初始状态下每个District中，最老的undeliveryed订单是2101
            undelivered_order[i][j] = 2101;
        }
    }
}

TPCCWorkload::~TPCCWorkload()
{

}


Transaction* TPCCWorkload::GenTxn()
{
    uint64_t rand = UtilFunc::Rand(TXN_TOTAL_RATIO, 0);
    Transaction* txn = nullptr;

    if (rand < NEWORDER_RATIO)
        txn = new NewOrder(TPCCTxnType::NEW_ORDER_TXN);
    else if (rand < PAYMENT_RATIO)
        txn = new Payment(TPCCTxnType::PAYMENT_TXN);
    else if (rand < DELIVERY_RATIO)
        txn = new Delivery(TPCCTxnType::DELIVERY_TXN);

    return txn;
}


void TPCCWorkload::InsertNewTxn(ThreadID thread_id, Transaction* new_txn)
{
    txns_per_threads_[thread_id].push(new_txn);
}


/********** NewOrder ***********/
NewOrder::NewOrder()
{

}

NewOrder::NewOrder(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

NewOrder::NewOrder(ThreadID thread_id, uint64_t local_txn_id):Transaction(thread_id, local_txn_id)
{
    
}


NewOrder::~NewOrder()
{
    delete[] ol_i_ids_;
    delete[] ol_supply_w_ids_;
    delete[] ol_quantities_;
}


void NewOrder::GenInputData()
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = UtilFunc::URand(1, g_warehouse_num, thread_id);
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
	c_id_ = TPCCUtilFunc::NURand(1023, 1, g_cust_per_dist, thread_id);
    
    ol_cnt_    = UtilFunc::URand(5, 15, thread_id);
	o_entry_d_ = 2023;

    ol_i_ids_        = new uint64_t[ol_cnt_];
    ol_supply_w_ids_ = new uint64_t[ol_cnt_];
    ol_quantities_    = new uint64_t[ol_cnt_];

    remote_ = 0;

    is_local_txn_   = true;
    local_shard_id_ = w_id_ - 1;
    
    uint64_t remote_w_id = 0;
    if (g_warehouse_num == 1)
        remote_w_id = w_id_;
    else
        while ((remote_w_id = UtilFunc::URand(1, g_warehouse_num, thread_id)) == w_id_){}


    for (uint32_t i = 0; i < ol_cnt_; i++)
    {
        ol_i_ids_[i] = TPCCUtilFunc::NURand(8191, 1, g_item_num, thread_id);
        
    #if  DISTRIBUTED_TXN
        uint64_t rand = UtilFunc::URand(1, 100, thread_id);
        //每一条orderline有99%的概率访问本地的stock
        //如果warehouse总数为1，则不存在分布式事务
        if (rand > g_new_order_remote_ratio || g_warehouse_num == 1)
            ol_supply_w_ids_[i] = w_id_;
        else
        {
            ol_supply_w_ids_[i] = remote_w_id;
            is_local_txn_ = false;
            remote_ = 1;
        }
    #else
        ol_supply_w_ids_[i] = w_id_;
    #endif

        ol_quantities_[i] = UtilFunc::URand(1, 10, thread_id);
    }
    
    //TPCC负载按照warehouse划分shard，当前实现每个warehouse一个shard
    //TODO: 如果实现更复杂的分区方式，则需要提供warehouse和shard的映射方式。
    if (is_local_txn_ == false)
        remote_shard_id_ = remote_w_id - 1;
    

	// 同一个订单的不同orderline中不能有重复的item
	for (uint32_t i = 0; i < ol_cnt_; i ++) 
    {
        bool duplicate = false;
        do
        {
            duplicate = false;

            for (uint32_t j = 0; j < i; j++) {
			    if (ol_i_ids_[i] == ol_i_ids_[j]) {
                    duplicate = true;
                    break;
			    }
		    }

            if (duplicate) {
                ol_i_ids_[i] = TPCCUtilFunc::NURand(8191, 1, g_item_num, thread_id);
            }
        } while (duplicate);
	}
}


void NewOrder::GenInputData(uint64_t  ware_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = ware_id;
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
	c_id_ = TPCCUtilFunc::NURand(1023, 1, g_cust_per_dist, thread_id);
    
    ol_cnt_    = UtilFunc::URand(g_min_ol_num_per_order, g_max_ol_num_per_order, thread_id);
	o_entry_d_ = 2023;

    ol_i_ids_        = new uint64_t[ol_cnt_];
    ol_supply_w_ids_ = new uint64_t[ol_cnt_];
    ol_quantities_    = new uint64_t[ol_cnt_];

    remote_ = 0;

    is_local_txn_   = true;
    local_shard_id_ = w_id_ - 1;
    
    uint64_t remote_w_id = 0;
    if (g_warehouse_num == 1)
        remote_w_id = w_id_;
    else
        while ((remote_w_id = UtilFunc::URand(1, g_warehouse_num, thread_id)) == w_id_){}


    for (uint32_t i = 0; i < ol_cnt_; i++)
    {
        ol_i_ids_[i] = TPCCUtilFunc::NURand(8191, 1, g_item_num, thread_id);
        
    #if  DISTRIBUTED_TXN
        uint64_t rand = UtilFunc::URand(1, 100, thread_id);
        //每一条orderline有99%的概率访问本地的stock
        //如果warehouse总数为1，则不存在分布式事务
        if (rand > g_new_order_remote_ratio || g_warehouse_num == 1)
            ol_supply_w_ids_[i] = w_id_;
        else
        {
            ol_supply_w_ids_[i] = remote_w_id;
            is_local_txn_ = false;
            remote_ = 1;
        }

    #else
        ol_supply_w_ids_[i] = w_id_;
    #endif

        ol_quantities_[i] = UtilFunc::URand(1, 10, thread_id);
    }
    
    //TPCC负载按照warehouse划分shard，当前实现每个warehouse一个shard
    //TODO: 如果实现更复杂的分区方式，则需要提供warehouse和shard的映射方式。
    if (is_local_txn_ == false)
        remote_shard_id_ = remote_w_id - 1;
    

	// 同一个订单的不同orderline中不能有重复的item
	for (uint32_t i = 0; i < ol_cnt_; i ++) 
    {
        bool duplicate = false;
        do
        {
            duplicate = false;

            for (uint32_t j = 0; j < i; j++) {
			    if (ol_i_ids_[i] == ol_i_ids_[j]) {
                    duplicate = true;
                    break;
			    }
		    }

            if (duplicate) {
                ol_i_ids_[i] = TPCCUtilFunc::NURand(8191, 1, g_item_num, thread_id);
            }
        } while (duplicate);
	}
}


RC NewOrder::RunTxn()
{
    double   w_tax;
    uint64_t d_next_o_id;
    double   d_tax;
    double   c_discount;
    char*    c_last;
    char*    c_credit;
    uint64_t o_id;

    RC rc;
    
    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;

    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];

    Tuple*      origin_tuple = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple = nullptr;
    TupleData   tuple_data  = nullptr;
    ColumnData  column_data = nullptr;


    /*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/

    // printf("1\n");

    /** Warehouse:
     *      w_tax **/

    index = g_schema->GetIndex(W_PK_INDEX, w_id_-1);

    index_attr[0] = &w_id_;
    index_key = g_schema->GetIndexKey(W_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);

    if (rc != RC_OK)
    {
        return RC_ABORT;
    }
    
    // printf("1.1\n");


    index_and_opt_cnt = 0;
    //不记录索引read操作，read操作不对索引造成物理变化
    // index_and_opts[0].index_id_  = W_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::READ_AT;

    rc = txn_context_->AccessTuple(AccessType::READ_AT, WAREHOUSE_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(TPCCTableType::WAREHOUSE_T, WarehouseCol::W_TAX, tuple_data, column_data);
    w_tax = *(double*)column_data;


    /** Customer: 
     *      c_discount, c_last, c_credit **/
    // printf("2\n");

    index = g_schema->GetIndex(C_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_attr[2] = &c_id_;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    rc = index->IndexRead(index_key, origin_tuple);

    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    //不记录索引read操作，read操作不对索引造成物理变化
    // index_and_opts[0].index_id_  = C_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::READ_AT;

    rc = txn_context_->AccessTuple(AccessType::READ_AT, CUSTOMER_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    

    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(TPCCTableType::CUSTOMER_T, CustomerCol::C_DISCOUNT, tuple_data, column_data);
    c_discount = *(double*)column_data;

    g_schema->GetColumnValue(TPCCTableType::CUSTOMER_T, CustomerCol::C_LAST, tuple_data, column_data);
    c_last = (char*)column_data;

    g_schema->GetColumnValue(TPCCTableType::CUSTOMER_T, CustomerCol::C_CREDIT, tuple_data, column_data);
    c_credit = (char*)column_data;


	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE district SET d _next_o_id = :d _next_o_id + 1
		WHERE d _id = :d_id AND d _w _id = :w _id ;
	+===================================================*/

    /** District:
     *      d_tax, d_next_o_id **/
    // printf("3\n");

    index = g_schema->GetIndex(D_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_key = g_schema->GetIndexKey(D_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);

    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    //不记录索引read操作，read操作不对索引造成物理变化
    // index_and_opts[0].index_id_  = D_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::READ_AT;

    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, DISTRICT_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    
    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(TPCCTableType::DISTRICT_T, DistrictCol::D_NEXT_O_ID, tuple_data, column_data);
    d_next_o_id = *(uint64_t *)column_data;

    g_schema->GetColumnValue(TPCCTableType::DISTRICT_T, DistrictCol::D_TAX, tuple_data, column_data);
    d_tax = *(double*)column_data;

    o_id = d_next_o_id;
    d_next_o_id++;
    g_schema->SetColumnValue(TPCCTableType::DISTRICT_T, DistrictCol::D_NEXT_O_ID, tuple_data, (ColumnData)&d_next_o_id);


	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/

#if TPCC_INSERT
    // printf("4\n");

    new_tuple = new Tuple(g_schema->GetTupleSize(TPCCTableType::ORDER_T));
    tuple_data = new_tuple->GetTupleData();
    
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_ID, tuple_data, (ColumnData)&o_id);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_D_ID, tuple_data, (ColumnData)&d_id_);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_W_ID, tuple_data, (ColumnData)&w_id_);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_C_ID, tuple_data, (ColumnData)&c_id_);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_ENTRY_D, tuple_data, (ColumnData)&o_entry_d_);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_OL_CNT, tuple_data, (ColumnData)&ol_cnt_);
    g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_ALL_LOCAL, tuple_data, (ColumnData)&remote_);


    index_and_opt_cnt = 1;
    //IndexAndOpt只记录二级索引变更操作
    // index_and_opts[0].index_id_  = O_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::INSERT_AT;
    index_and_opts[0].index_id_  = O_CUST_INDEX;
    index_and_opts[0].index_opt_ = AccessType::INSERT_AT;

    rc = txn_context_->AccessTuple(AccessType::INSERT_AT, ORDER_T, w_id_-1, index_and_opt_cnt, index_and_opts, new_tuple, operate_tuple);

    // printf("4.1\n");
    if (rc == RC::RC_ABORT)
    {
        // delete new_tuple;  
        return RC::RC_ABORT;
    }
    
    index = g_schema->GetIndex(O_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_attr[2] = &o_id;
    index_key = g_schema->GetIndexKey(O_PK_INDEX, index_attr);

    rc = index->IndexInsert(index_key, operate_tuple);

    // printf("4.2\n");
    if (rc != RC::RC_OK)
    {
        // delete new_tuple;     //?????
        return RC::RC_ABORT;
    }
#endif

    /*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
#if TPCC_INSERT
    // printf("5\n");

    new_tuple = new Tuple(g_schema->GetTupleSize(TPCCTableType::NEW_ORDER_T));
    tuple_data = new_tuple->GetTupleData();

    g_schema->SetColumnValue(TPCCTableType::NEW_ORDER_T, NewOrderCol::NO_O_ID, tuple_data, (ColumnData)&o_id);
    g_schema->SetColumnValue(TPCCTableType::NEW_ORDER_T, NewOrderCol::NO_D_ID, tuple_data, (ColumnData)&d_id_);
    g_schema->SetColumnValue(TPCCTableType::NEW_ORDER_T, NewOrderCol::NO_W_ID, tuple_data, (ColumnData)&w_id_);


    index_and_opt_cnt = 0;
    //IndexAndOpt只记录二级索引变更操作
    // index_and_opts[0].index_id_  = NO_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::INSERT_AT;

    rc = txn_context_->AccessTuple(AccessType::INSERT_AT, NEW_ORDER_T, w_id_-1, index_and_opt_cnt, index_and_opts, new_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        return RC::RC_ABORT;
    }

    index = g_schema->GetIndex(NO_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_attr[2] = &o_id;
    index_key = g_schema->GetIndexKey(NO_PK_INDEX, index_attr);

    rc = index->IndexInsert(index_key, operate_tuple);

    if (rc != RC::RC_OK)
    {
        // delete new_tuple;     //?????
        return RC::RC_ABORT;
    }
    
#endif

    // printf("6\n");

    Index* item_index  = g_schema->GetIndex(I_PK_INDEX, w_id_-1);
    Index* ol_index    = g_schema->GetIndex(OL_PK_INDEX, w_id_-1);
    Index* stock_index = nullptr;

    uint64_t ol_i_id        = 0;
    uint64_t ol_supply_w_id = 0;
    uint64_t ol_quantity    = 0;

    for (uint64_t  ol_number = 1; ol_number <= ol_cnt_; ol_number++)
    {
        ol_i_id        = ol_i_ids_[ol_number - 1];
        ol_supply_w_id = ol_supply_w_ids_[ol_number - 1];
        ol_quantity    = ol_quantities_[ol_number - 1];

        //使用ol_supply_w_id，因为可能存在分布式事务，orderline访问远端的stock
        stock_index = g_schema->GetIndex(S_PK_INDEX, ol_supply_w_id-1);


        /*===========================================+
        EXEC SQL SELECT i_price, i_name , i_data
            INTO :i_price, :i_name, :i_data
            FROM item
            WHERE i_id = :ol_i_id;
        +===========================================*/
        // printf("6.1\n");
        uint64_t i_price;
        char*    i_name;
        char*    i_data;

        index_attr[0] = &ol_i_id;
        index_key = g_schema->GetIndexKey(I_PK_INDEX, index_attr);

        rc = item_index->IndexRead(index_key, origin_tuple);


        index_and_opt_cnt = 0;
        //不记录索引read操作，read操作不对索引造成物理变化
        // index_and_opts[0].index_id_  = I_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::READ_AT;

        rc = txn_context_->AccessTuple(AccessType::READ_AT, ITEM_T, ol_supply_w_id-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

        if (rc == RC_ABORT)
        {
            return RC::RC_ABORT;
        }

        tuple_data = operate_tuple->GetTupleData();

        g_schema->GetColumnValue(TPCCTableType::ITEM_T, ItemCol::I_PRICE, tuple_data, column_data);
        i_price = *(uint64_t *)column_data;

        g_schema->GetColumnValue(TPCCTableType::ITEM_T, ItemCol::I_NAME, tuple_data, column_data);
        i_name = column_data;

        g_schema->GetColumnValue(TPCCTableType::ITEM_T, ItemCol::I_DATA, tuple_data, column_data);
        i_data = column_data;


		/*==============================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+==============================================================*/
        // printf("6.2 thread id: %ld\n", txn_identifier_->thread_id_);
        uint64_t s_quantity;
        char*    s_data;
        char*    s_dist_info;

        //stock主键使用 ol_supply_w_id 而非 w_id_
        index_attr[0] = &ol_supply_w_id;
        index_attr[1] = &ol_i_id;
        index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);

        rc = stock_index->IndexRead(index_key, origin_tuple);

        if (rc == RC::RC_NULL)
        {
            return RC::RC_ABORT;
        }


        index_and_opt_cnt = 0;
        //不记录索引read操作，read操作不对索引造成物理变化
        // index_and_opts[0].index_id_  = S_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::READ_AT;

        txn_context_->AccessTuple(AccessType::UPDATE_AT, STOCK_T, ol_supply_w_id-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

        // !!!!!!分布式事务下，有可能在访问stock时出现冲突，造成回滚。
        if (rc == RC_ABORT)
        {
            return RC::RC_ABORT;
        }

        tuple_data = operate_tuple->GetTupleData();

        g_schema->GetColumnValue(TPCCTableType::STOCK_T, StockCol::S_DATA, tuple_data, column_data);
        s_data = (char*)column_data;

        g_schema->GetColumnValue(TPCCTableType::STOCK_T, (ColumnID)(StockCol::S_DIST_01 + d_id_ - 1), tuple_data, column_data);
        s_dist_info = (char*)column_data;

        g_schema->GetColumnValue(TPCCTableType::STOCK_T, StockCol::S_QUANTITY, tuple_data, column_data);
        s_quantity = *(uint64_t *)column_data;

        if (s_quantity > ol_quantity + 10)
        {
			s_quantity = s_quantity - ol_quantity;
		} 
        else
        {
			s_quantity = s_quantity - ol_quantity + 91;
		}

        g_schema->SetColumnValue(TPCCTableType::STOCK_T, StockCol::S_QUANTITY, tuple_data, (ColumnData)&s_quantity);
        

		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
#if TPCC_INSERT

        // printf("6.3 thread id: %ld\n", txn_identifier_->thread_id_);
        new_tuple = new Tuple(g_schema->GetTupleSize(TPCCTableType::ORDER_LINE_T));

        tuple_data = new_tuple->GetTupleData();

        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_O_ID, tuple_data, (ColumnData)&o_id);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_D_ID, tuple_data, (ColumnData)&d_id_);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_W_ID, tuple_data, (ColumnData)&w_id_);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_NUMBER, tuple_data, (ColumnData)&ol_number);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_I_ID, tuple_data, (ColumnData)&ol_i_id);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_SUPPLY_W_ID, tuple_data, (ColumnData)&ol_supply_w_id);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_QUANTITY, tuple_data, (ColumnData)&ol_quantity);


        index_and_opt_cnt = 0;
        //IndexAndOpt只记录二级索引变更操作
        // index_and_opts[0].index_id_  = OL_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::INSERT_AT;

        rc = txn_context_->AccessTuple(AccessType::INSERT_AT, ORDER_LINE_T, w_id_-1, index_and_opt_cnt, index_and_opts, new_tuple, operate_tuple);

        if (rc == RC_ABORT)
        {
            // delete new_tuple;     //??
            return RC::RC_ABORT;
        }

        index_attr[0] = &w_id_;    //主键，一定要使用w_id_，不能使用ol_supply_w_id
        index_attr[1] = &d_id_;
        index_attr[2] = &o_id;
        index_attr[3] = &ol_number;
        index_key = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);

        rc = ol_index->IndexInsert(index_key, operate_tuple);

        // printf("6.4 thread id: %ld\n", txn_identifier_->thread_id_);

        if (rc != RC::RC_OK)
        {
            // delete new_tuple;     //??????
            return RC::RC_ABORT;
        }

        uint64_t ol_amount = 0;
        ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_AMOUNT, tuple_data, (ColumnData)&ol_amount);
        g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_DIST_INFO, tuple_data, (ColumnData)s_dist_info);

#endif

    }


    // printf("w_tax: %lf, c_discount: %lf, d_next_o_id: %ld, d_tax: %lf \n ", w_tax, c_discount, d_next_o_id, d_tax);
    // printf("7\n");
    return RC::RC_COMMIT;

}



Payment::Payment()
{
    
}


Payment::Payment(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

Payment::~Payment()
{
}


void Payment::GenInputData()
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = UtilFunc::URand(1, g_warehouse_num, thread_id);
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
    
    h_date_   = 2023;
    h_amount_ = UtilFunc::URand(1, 5000, thread_id);
    

    by_last_name_ = false;

    is_local_txn_   = true;
    local_shard_id_ = w_id_; 

#if  DISTRIBUTED_TXN

    uint64_t rand = UtilFunc::URand(1, 100, thread_id);
    if (rand <= g_payment_remote_ratio)
    {
        c_w_id_ = w_id_;
        c_d_id_ = d_id_;
    }
    else
    {
        c_d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);

        if (g_warehouse_num > 1)
        {
            while ((c_w_id_ = UtilFunc::URand(1, g_warehouse_num, thread_id)) == w_id_)
            { ; }

            is_local_txn_ = false;   
        }
        else {
            c_w_id_ = w_id_;
        }
    }
#else
    c_w_id_ = w_id_;
    c_d_id_ = d_id_;
#endif
    
    if (is_local_txn_ == false)
        remote_shard_id_ = c_w_id_ - 1;
    

    if (by_last_name_)
    {
        //TODO: database support query tuple by secondary index
    }
    else
    {
        c_id_ = TPCCUtilFunc::NURand(1023, 1, g_cust_per_dist, thread_id);
    }
}

void Payment::GenInputData(uint64_t  ware_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = ware_id;
    d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
    
    h_date_   = 2023;
    h_amount_ = UtilFunc::URand(1, 5000, thread_id);
    
    by_last_name_ = false;

    is_local_txn_   = true;
    local_shard_id_ = w_id_; 

#if  DISTRIBUTED_TXN
    uint64_t rand = UtilFunc::URand(1, 100, thread_id);
    if (rand <= g_payment_remote_ratio)
    {
        /* 分布式事务 */
        c_d_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);

        if (g_warehouse_num > 1)
        {
            while ((c_w_id_ = UtilFunc::URand(1, g_warehouse_num, thread_id)) == w_id_)
            { ; }

            is_local_txn_ = false; 
        }
        else {
            c_w_id_ = w_id_;
        }
    }
    else
    {
        /* 本地事务 */
        c_w_id_ = w_id_;
        c_d_id_ = d_id_;
    }
#else
    c_w_id_ = w_id_;
    c_d_id_ = d_id_;
#endif
    
    if (is_local_txn_ == false)
        remote_shard_id_ = c_w_id_ - 1;
    

    if (by_last_name_)
    {
        //TODO: database support query tuple by secondary index
    }
    else
    {
        c_id_ = TPCCUtilFunc::NURand(1023, 1, g_cust_per_dist, thread_id);
    }
}


RC Payment::RunTxn()
{
    RC rc = RC_COMMIT;

    uint64_t w_ytd;
    char     w_name[11];

    uint64_t d_ytd;
    char     d_name[11];

    uint64_t c_balance;
    uint64_t c_ytd_payment;
    uint64_t c_payment_cnt;
    char     c_credit[3];

    uint64_t h_id;
    char     h_data[25];


    Index*     index = nullptr;
    void*      index_attr[5];
    IndexKey   index_key;

    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];

    Tuple*     origin_tuple  = nullptr;
    Tuple*     operate_tuple = nullptr;
    Tuple*     new_tuple     = nullptr;

    TupleData  tuple_data    = nullptr;
    ColumnData column_data   = nullptr;


	/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*======================================================
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/

    index         = g_schema->GetIndex(W_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_key     = g_schema->GetIndexKey(W_PK_INDEX, index_attr);

    index->IndexRead(index_key, origin_tuple);


    index_and_opt_cnt = 0;
    //不记录索引read操作，read操作不对索引造成物理变化
    // index_and_opts[0].index_id_  = W_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::READ_AT;

    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, WAREHOUSE_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return rc;

    
    g_schema->GetColumnValue(WAREHOUSE_T, W_YTD, operate_tuple->tuple_data_, column_data);
    w_ytd = *(uint64_t *)column_data;
    w_ytd += h_amount_;
    g_schema->SetColumnValue(WAREHOUSE_T, W_YTD, operate_tuple->tuple_data_, (ColumnData)&w_ytd);

    g_schema->GetColumnValue(WAREHOUSE_T, W_NAME, operate_tuple->tuple_data_, column_data);
    memcpy(w_name, (char*)column_data, 10);
    w_name[10] = '\0';


	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

    index = g_schema->GetIndex(D_PK_INDEX, w_id_-1);
    index_attr[0] = &w_id_;
    index_attr[1] = &d_id_;
    index_key = g_schema->GetIndexKey(D_PK_INDEX, index_attr);

    index->IndexRead(index_key, origin_tuple);


    index_and_opt_cnt = 0;
    //不记录索引read操作，read操作不对索引造成物理变化
    // index_and_opts[0].index_id_  = D_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::READ_AT;

    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, DISTRICT_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return rc;


    g_schema->GetColumnValue(DISTRICT_T, D_YTD, operate_tuple->tuple_data_, column_data);
    d_ytd = *(uint64_t *)column_data;
    d_ytd += h_amount_;
    g_schema->SetColumnValue(DISTRICT_T, D_YTD, operate_tuple->tuple_data_, (ColumnData)&d_ytd);

    g_schema->GetColumnValue(DISTRICT_T, D_YTD, operate_tuple->tuple_data_, column_data);
    memcpy(d_name, (char*)column_data, 10);
    d_name[10] = '\0';


    if (by_last_name_)
    {
        ;
    }
    else
    {
        /*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/

        index = g_schema->GetIndex(C_PK_INDEX, c_w_id_-1);
        //要使用c_相关属性，不能是w_id_、 d_id_，因为分布式事务下，customer有可能是remote
        index_attr[0] = &c_w_id_;
        index_attr[1] = &c_d_id_;
        index_attr[2] = &c_id_;
        index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);

        index->IndexRead(index_key, origin_tuple);


        index_and_opt_cnt = 0;
        //不记录索引read操作，read操作不对索引造成物理变化
        // index_and_opts[0].index_id_  = C_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::READ_AT;

        rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, CUSTOMER_T, c_w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
        
        if (rc == RC_ABORT)
            return rc;
    }
    

  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/

    g_schema->GetColumnValue(CUSTOMER_T, C_BALANCE, operate_tuple->tuple_data_, column_data);
    c_balance = *(uint64_t *)column_data;
    c_balance -= h_amount_;
    g_schema->SetColumnValue(CUSTOMER_T, C_BALANCE, operate_tuple->tuple_data_, (ColumnData)&c_balance);

    g_schema->GetColumnValue(CUSTOMER_T, C_YTD_PAYMENT, operate_tuple->tuple_data_, column_data);
    c_ytd_payment = *(uint64_t *)column_data;
    c_ytd_payment += h_amount_;
    g_schema->SetColumnValue(CUSTOMER_T, C_YTD_PAYMENT, operate_tuple->tuple_data_, (ColumnData)&c_ytd_payment);

    g_schema->GetColumnValue(CUSTOMER_T, C_PAYMENT_CNT, operate_tuple->tuple_data_, column_data);
    c_payment_cnt = *(uint64_t *)column_data;
    c_payment_cnt++;
    g_schema->SetColumnValue(CUSTOMER_T, C_PAYMENT_CNT, operate_tuple->tuple_data_, (ColumnData)&c_payment_cnt);

    g_schema->GetColumnValue(CUSTOMER_T, C_CREDIT, operate_tuple->tuple_data_, column_data);
    memcpy(c_credit, column_data, 2);
    c_credit[2] = '\0';

    if (strstr(c_credit, "BC"))
    {
        /*=====================================================+
		    EXEC SQL SELECT c_data
			INTO :c_data
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+=====================================================*/


    }
    

	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
#if TPCC_INSERT    
    strncpy(h_data, w_name, 10);
    strcpy(&h_data[10], "    ");
    strncpy(&h_data[14], d_name, 10);
    h_data[24] = '\0';

    h_id = g_schema->FetchIncID(HISTORY_T);

    new_tuple = new Tuple(g_schema->GetTupleSize(HISTORY_T));
    g_schema->SetColumnValue(HISTORY_T, H_ID, new_tuple->tuple_data_, (ColumnData)&h_id);
    g_schema->SetColumnValue(HISTORY_T, H_W_ID, new_tuple->tuple_data_, (ColumnData)&w_id_);
    g_schema->SetColumnValue(HISTORY_T, H_D_ID, new_tuple->tuple_data_, (ColumnData)&d_id_);
    g_schema->SetColumnValue(HISTORY_T, H_C_ID, new_tuple->tuple_data_, (ColumnData)&c_id_);
    g_schema->SetColumnValue(HISTORY_T, H_C_W_ID, new_tuple->tuple_data_, (ColumnData)&c_w_id_);
    g_schema->SetColumnValue(HISTORY_T, H_C_D_ID, new_tuple->tuple_data_, (ColumnData)&c_d_id_);
    g_schema->SetColumnValue(HISTORY_T, H_DATE, new_tuple->tuple_data_, (ColumnData)&h_date_);
    g_schema->SetColumnValue(HISTORY_T, H_DATA, new_tuple->tuple_data_, (ColumnData)&h_data);
    g_schema->SetColumnValue(HISTORY_T, H_AMOUNT, new_tuple->tuple_data_, (ColumnData)&h_amount_);

    
    index_and_opt_cnt = 0;
    //IndexAndOpt只记录二级索引变更操作
    // index_and_opts[0].index_id_  = H_PK_INDEX;
    // index_and_opts[0].index_opt_ = AccessType::INSERT_AT;

    rc = txn_context_->AccessTuple(AccessType::INSERT_AT, HISTORY_T, w_id_-1, index_and_opt_cnt, index_and_opts, new_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
        return rc;

    index = g_schema->GetIndex(H_PK_INDEX, w_id_-1);
    index_attr[0] = &h_id;
    index_key = g_schema->GetIndexKey(H_PK_INDEX, index_attr);

    index->IndexInsert(index_key, new_tuple);

#endif


    return RC_COMMIT;    

}


/*****************************************/
/*************** Delivery ****************/
/*****************************************/
Delivery::Delivery()
{

}

Delivery::Delivery(TPCCTxnType txn_type)
{
    txn_type_ = txn_type;
}

Delivery::~Delivery()
{

}

void Delivery::GenInputData()
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = UtilFunc::URand(1, g_warehouse_num, thread_id);
    o_carrier_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
    ol_delivery_d_ = 2024;

    //Delivery事务一定是本地事务
    is_local_txn_ = true;
    local_shard_id_ = w_id_ - 1;
}

void Delivery::GenInputData(uint64_t  ware_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    w_id_ = ware_id;
    o_carrier_id_ = UtilFunc::URand(1, g_dist_per_ware, thread_id);
    ol_delivery_d_ = 2024;

    //Delivery事务一定是本地事务
    is_local_txn_ = true;
    local_shard_id_ = w_id_ - 1;

}


RC Delivery::RunTxn()
{
    //保证同一个warehouse同一时刻只能运行一个delivery事务
    while (!ATOM_CAS(((TPCCWorkload*)g_workload)->deliverying[w_id_], false, true))
    {
        ;
    }
    
    RC rc;
    
    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;

    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];

    Tuple*      origin_tuple = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple = nullptr;
    TupleData   tuple_data  = nullptr;
    ColumnData  column_data = nullptr;


    bool     find[g_dist_per_ware + 1];
    uint64_t d_id;
    for (d_id = 1; d_id <= g_dist_per_ware; d_id++)
    {
        find[d_id] = false;

        /********************************************************
         * 找到该district下，最老还未delivery的order id。
         * 将对应的new_order元组删除
         *******************************************************/
        uint64_t o_id = ((TPCCWorkload*)g_workload)->undelivered_order[w_id_][d_id];

        index = g_schema->GetIndex(NO_PK_INDEX, w_id_-1);
        
        if (w_id_ == 2 && d_id == 1 && o_id == 2101)
        {
            uint64_t a = 0;
        }

        index_attr[0] = &w_id_;
        index_attr[1] = &d_id;
        index_attr[2] = &o_id;
        index_key = g_schema->GetIndexKey(NO_PK_INDEX, index_attr);

        rc = index->IndexRead(index_key, origin_tuple);

        //未找到对应的new_order，说明该district下没有新插入的订单
        if (rc == RC_NULL)
            continue;

        //找到了对应的new_order
        find[d_id] = true;


        index_and_opt_cnt = 0;
        //IndexAndOpt只记录二级索引变更操作
        // index_and_opts[0].index_id_  = NO_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::DELETE_AT;

        rc = txn_context_->AccessTuple(AccessType::DELETE_AT, NEW_ORDER_T, w_id_ - 1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

        if (rc == RC_ABORT)
            goto abort_txn;
        

        /*************************************************
         * 根据w_id d_id o_id查找对应的order
         * 获取o_c_id，并更新o_carrier_id_
         *************************************************/
        index = g_schema->GetIndex(O_PK_INDEX, w_id_ - 1);

        index_attr[0] = &w_id_;
        index_attr[1] = &d_id;
        index_attr[2] = &o_id;
        index_key = g_schema->GetIndexKey(O_PK_INDEX, index_attr);

        rc = index->IndexRead(index_key, origin_tuple);

        if (rc != RC_OK)
            goto abort_txn;
        

        index_and_opt_cnt = 0;
        //不记录索引read操作，read操作不对索引造成物理变化
        // index_and_opts[0].index_id_  = O_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::READ_AT;

        rc = txn_context_->AccessTuple(UPDATE_AT, ORDER_T, w_id_ - 1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

        if (rc == RC_ABORT)
            goto abort_txn;

        tuple_data = operate_tuple->GetTupleData();

        uint64_t o_c_id = 0;

        g_schema->GetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_C_ID, tuple_data, column_data);
        o_c_id = *(uint64_t *)column_data;

        g_schema->SetColumnValue(TPCCTableType::ORDER_T, OrderCol::O_CARRIER_ID, tuple_data, (ColumnData)&o_carrier_id_);


        /*************************************************************
         * 查找Order对应的Order-line，
         * 对每个Order-line 更新 ol_delivery_d 属性，计算ol_amount总和
         ************************************************************/
        uint64_t ol_total = 0;

        index = g_schema->GetIndex(OL_PK_INDEX, w_id_ - 1);
        index_attr[0] = &w_id_;
        index_attr[1] = &d_id;
        index_attr[2] = &o_id;

        /* 
         * 每个order最多有15个ordeline，因此直接遍历。
         * 直到遍历结束，或者没有找到对应的order_line，退出遍历。
         */
        for (uint64_t ol_number = 1; ol_number <= 15; ol_number++)
        {
            index_attr[3] = &ol_number;
            index_key = g_schema->GetIndexKey(OL_PK_INDEX, index_attr);
            
            rc = index->IndexRead(index_key, origin_tuple);

            //未找到对应的order-line，退出遍历
            if (rc == RC_NULL)
            {
                break;
            }


            index_and_opt_cnt = 0;
            //不记录索引read操作，read操作不对索引造成物理变化
            // index_and_opts[0].index_id_  = OL_PK_INDEX;
            // index_and_opts[0].index_opt_ = AccessType::READ_AT;

            rc = txn_context_->AccessTuple(UPDATE_AT, ORDER_LINE_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

            if (rc == RC_ABORT)
                goto abort_txn;
            
            uint64_t ol_amount = 0;
            
            tuple_data = operate_tuple->GetTupleData();

            g_schema->GetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_AMOUNT, tuple_data, column_data);
            ol_amount = *(uint64_t *)column_data;
            ol_total += ol_amount;

            g_schema->SetColumnValue(TPCCTableType::ORDER_LINE_T, OrderLineCol::OL_DELIVERY_D, tuple_data, (ColumnData)&ol_delivery_d_);
        }

        /**********************************
         * 根据order的O_C_ID属性，确定订单所属customer。
         * 更新c_balance属性，+ ol_total
         **********************************/
        index = g_schema->GetIndex(C_PK_INDEX, w_id_-1);
        
        index_attr[0] = &w_id_;
        index_attr[1] = &d_id;
        index_attr[2] = &o_c_id;
        index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);

        rc = index->IndexRead(index_key, origin_tuple);
        if (rc != RC_OK)
            goto abort_txn;


        index_and_opt_cnt = 0;
        //不记录索引read操作，read操作不对索引造成物理变化
        // index_and_opts[0].index_id_  = C_PK_INDEX;
        // index_and_opts[0].index_opt_ = AccessType::READ_AT;

        rc = txn_context_->AccessTuple(UPDATE_AT, CUSTOMER_T, w_id_-1, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
        if (rc == RC_ABORT)
            goto abort_txn;
        
        uint64_t c_balance = 0;
        tuple_data = operate_tuple->GetTupleData();

        g_schema->GetColumnValue(TPCCTableType::CUSTOMER_T, CustomerCol::C_BALANCE, tuple_data, column_data);
        c_balance = *(uint64_t *)column_data;

        c_balance += ol_total;
        g_schema->SetColumnValue(TPCCTableType::CUSTOMER_T, CustomerCol::C_BALANCE, tuple_data, (ColumnData)&c_balance);
    }
    

    //对于处理了new_order的district，将其undelivered的order id + 1
    for (uint64_t  i = 1; i <= g_dist_per_ware; i++)
    {
        if (find[i])
            ((TPCCWorkload*)g_workload)->undelivered_order[w_id_][i]++;
    }


commit_txn:

    ATOM_CAS(((TPCCWorkload*)g_workload)->deliverying[w_id_], true, false);
    return RC_COMMIT;

abort_txn:

    ATOM_CAS(((TPCCWorkload*)g_workload)->deliverying[w_id_], true, false);
    return RC_ABORT;

}

#endif
