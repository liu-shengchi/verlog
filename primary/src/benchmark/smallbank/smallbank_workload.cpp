#include "smallbank_workload.h"
#include "smallbank_schema.h"
#include "smallbank_config.h"

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


#if   WORKLOAD_TYPE == SMALLBANK_W


/******** SmallBankWorkload ********/

SmallBankWorkload::SmallBankWorkload()
{
    txn_thread_num_     = g_txn_thread_num;
    txn_num_per_thread_ = g_txns_num_per_thread;

    txns_per_threads_ = new std::queue<Transaction*>[txn_thread_num_];
}

SmallBankWorkload::~SmallBankWorkload()
{

}


Transaction* SmallBankWorkload::GenTxn()
{
    uint64_t rand = UtilFunc::Rand(g_txn_total_ratio, 0);
    Transaction* txn = nullptr;

    if (rand < g_balance_ratio)
        txn = new Balance(SMALLBANKTxnType::BALANCE_TXN);
    else if (rand < g_deposit_checking_ratio)
        txn = new DespositChecking(SMALLBANKTxnType::DEPOSIT_CHECKING_TXN);
    else if (rand < g_transact_saving_ratio)
        txn = new TransactSaving(SMALLBANKTxnType::TRANSACT_SAVING_TXN);
    else if (rand < g_amalgamate_ratio)
        txn = new Amalgamate(SMALLBANKTxnType::AMALGAMATE_TXN);
    else if (rand < g_write_check_ratio)
        txn = new WriteCheck(SMALLBANKTxnType::WRITE_CHECK_TXN);

    return txn;
}


void SmallBankWorkload::InsertNewTxn(ThreadID thread_id, Transaction* new_txn)
{
    txns_per_threads_[thread_id].push(new_txn);
}

/*********************************/
/************ Balance ************/
/*********************************/

Balance::Balance(SMALLBANKTxnType txn_type)
{
    txn_type_ = txn_type;
}


Balance::~Balance()
{
}


void Balance::GenInputData()
{

}


void Balance::GenInputData(ShardID shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_   = true;
    local_shard_id_ = shard_id;
    
    if ((UtilFunc::Rand(100, thread_id) / (double)100.0) < g_smallbank_skewness)
    {
        //average access hot account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
    }
    else
    {
        //average access all account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);
    }
}


RC Balance::RunTxn()
{
    uint64_t a_customer_id = 0;
    double   s_balance = 0;
    double   c_balance = 0;
    
    RC rc = RC_COMMIT;

    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;
    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];   //仅针对二级索引

    Tuple*      origin_tuple  = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple     = nullptr;
    TupleData   tuple_data    = nullptr;
    ColumnData  column_data   = nullptr;


    /*=======================================================================+
	EXEC SQL SELECT A_CUSTOMER_ID
		INTO :a_customer_id
		FROM ACCOUNT_T
		WHERE a_name =: a_name_;
	+========================================================================*/

    // printf("1\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);

    if (rc != RC_OK)
    {
        return RC_ABORT;
    }
    
    // printf("1.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id = *(uint64_t*)column_data;



    /*=======================================================================+
	EXEC SQL SELECT S_BALANCE
		INTO :s_balance
		FROM SAVING
		WHERE s_customer_id =: a_customer_id;
	+========================================================================*/

    // printf("2\n");

    index = g_schema->GetIndex(S_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, SMALLBANKTableType::SAVING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    
    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, column_data);
    s_balance = *(double*)column_data;



    /*=======================================================================+
	EXEC SQL SELECT C_BALANCE
		INTO :c_balance
		FROM CHECKING
		WHERE c_customer_id =: a_customer_id;
	+========================================================================*/

    // printf("3\n");

    index = g_schema->GetIndex(C_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, SMALLBANKTableType::CHECKING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    
    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, column_data);
    c_balance = *(double*)column_data;


    // printf("4\n");
    return RC::RC_COMMIT;

}




/*****************************************/
/*********** DespositChecking ************/
/*****************************************/

DespositChecking::DespositChecking(SMALLBANKTxnType txn_type)
{
    txn_type_ = txn_type;
}

DespositChecking::~DespositChecking()
{
}


void DespositChecking::GenInputData()
{

}

void DespositChecking::GenInputData(ShardID shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_   = true;
    local_shard_id_ = shard_id;

    if ((UtilFunc::Rand(100, thread_id) / (double)100.0) < g_smallbank_skewness)
    {
        //average access hot account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
    }
    else
    {
        //average access all account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);
    }

    c_balance_add_ = UtilFunc::Rand(10, thread_id);
}


RC DespositChecking::RunTxn()
{
    RC rc = RC_COMMIT;

    uint64_t a_customer_id = 0;
    double   c_balance     = 0.0;

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



    /*=======================================================================+
        EXEC SQL SELECT A_CUSTOMER_ID
            INTO :a_customer_id
            FROM ACCOUNT_T
            WHERE a_name =: a_name_;
	+========================================================================*/

    // printf("1\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;
    
    
    // printf("1.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id = *(uint64_t*)column_data;



    /*=======================================================================+
	   	EXEC SQL UPDATE checking SET c_balance =: c_balance
   		WHERE c_customer_id =: a_customer_id;
	+========================================================================*/

    // printf("2\n");

    index = g_schema->GetIndex(C_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;

    // printf("2.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::CHECKING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("2.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, column_data);
    c_balance = *(double*)column_data;

    c_balance += c_balance_add_;
    g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, (ColumnData)&c_balance);


    return RC_COMMIT;

}


/*****************************************/
/************ TransactSaving *************/
/*****************************************/

TransactSaving::TransactSaving(SMALLBANKTxnType txn_type)
{
    txn_type_ = txn_type;
}

TransactSaving::~TransactSaving()
{

}

void TransactSaving::GenInputData()
{
}

void TransactSaving::GenInputData(ShardID shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_ = true;
    local_shard_id_ = shard_id;

    if ((UtilFunc::Rand(100, thread_id) / (double)100.0) < g_smallbank_skewness)
    {
        //average access hot account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
    }
    else
    {
        //average access all account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);
    }

    s_balance_add_ = UtilFunc::Rand(10, thread_id);

}


RC TransactSaving::RunTxn()
{

    RC rc = RC::RC_COMMIT;
    
    uint64_t    a_customer_id = 0;
    double      s_balance     = 0.0;

    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;

    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];

    Tuple*      origin_tuple  = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple     = nullptr;
    TupleData   tuple_data    = nullptr;
    ColumnData  column_data   = nullptr;

    /*=======================================================================+
        EXEC SQL SELECT A_CUSTOMER_ID
            INTO :a_customer_id
            FROM ACCOUNT_T
            WHERE a_name =: a_name_;
	+========================================================================*/

    // printf("1\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;
    
    
    // printf("1.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id = *(uint64_t*)column_data;



    /*=======================================================================+
	   	EXEC SQL UPDATE checking SET s_balance =: s_balance
   		WHERE s_customer_id =: a_customer_id;
	+========================================================================*/

    // printf("2\n");

    index = g_schema->GetIndex(S_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;

    // printf("2.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::SAVING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("2.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, column_data);
    s_balance = *(double*)column_data;

    s_balance += s_balance_add_;
    g_schema->SetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, (ColumnData)&s_balance);


    return RC_COMMIT;

}




/*****************************************/
/************ Amalgamate *************/
/*****************************************/

Amalgamate::Amalgamate(SMALLBANKTxnType txn_type)
{
    txn_type_ = txn_type;
}

Amalgamate::~Amalgamate()
{

}

void Amalgamate::GenInputData()
{
}

void Amalgamate::GenInputData(ShardID shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_ = true;
    local_shard_id_ = shard_id;

    if ((UtilFunc::Rand(100, thread_id) / (double)100.0) < g_smallbank_skewness)
    {
        //average access hot account
        a_name_1_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
        
        do
        {
            a_name_2_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
        } while (a_name_2_ == a_name_1_);
    }
    else
    {
        //average access all account
        a_name_1_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);

        do
        {
            a_name_2_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);
        } while (a_name_2_ == a_name_1_);
    }

}


RC Amalgamate::RunTxn()
{
    RC rc = RC::RC_COMMIT;
    
    uint64_t    a_customer_id_1 = 0;
    double      s_balance_1     = 0.0;
    double      c_balance_1     = 0.0;
    double      total_balance   = 0.0;

    uint64_t    a_customer_id_2 = 0;
    double      c_balance_2     = 0.0;

    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;

    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];

    Tuple*      origin_tuple  = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple     = nullptr;
    TupleData   tuple_data    = nullptr;
    ColumnData  column_data   = nullptr;


    /*=======================================================================+
        EXEC SQL SELECT A_CUSTOMER_ID
            INTO :a_customer_id_1
            FROM ACCOUNT_T
            WHERE a_name =: a_name_1_;
	+========================================================================*/

    // printf("1\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_1_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;
    
    
    // printf("1.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id_1 = *(uint64_t*)column_data;



    /*=======================================================================+
	   	EXEC SQL UPDATE saving SET s_balance =: s_balance
   		WHERE s_customer_id =: a_customer_id_1;
	+========================================================================*/

    // printf("2\n");

    index = g_schema->GetIndex(S_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id_1;
    index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;

    // printf("2.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::SAVING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("2.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, column_data);
    s_balance_1 = *(double*)column_data;

    total_balance += s_balance_1;

    s_balance_1 = 0;
    g_schema->SetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, (ColumnData)&s_balance_1);



    /*=======================================================================+
	   	EXEC SQL UPDATE checking SET c_balance =: c_balance
   		WHERE c_customer_id =: a_customer_id_1;
	+========================================================================*/

    // printf("3\n");

    index = g_schema->GetIndex(C_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id_1;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;

    // printf("3.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::CHECKING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("3.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, column_data);
    c_balance_1 = *(double*)column_data;

    total_balance += c_balance_1;

    c_balance_1 = 0;
    g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, (ColumnData)&c_balance_1);



    /*=======================================================================+
        EXEC SQL SELECT A_CUSTOMER_ID
            INTO :a_customer_id_2
            FROM ACCOUNT_T
            WHERE a_name =: a_name_2_;
	+========================================================================*/

    // printf("4\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_2_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;
    
    
    // printf("4.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("4.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id_2 = *(uint64_t*)column_data;



    /*=======================================================================+
        EXEC SQL UPDATE checking SET c_balance =: c_balance_2
   		WHERE c_customer_id =: a_customer_id_2;
	+========================================================================*/

    // printf("5\n");

    index = g_schema->GetIndex(C_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id_2;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC_ABORT;

    // printf("5.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::CHECKING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("5.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, column_data);
    c_balance_2 = *(double*)column_data;

    c_balance_2 += total_balance;

    g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, (ColumnData)&c_balance_2);


    return RC_COMMIT;

}




/*********************************/
/************ Balance ************/
/*********************************/

WriteCheck::WriteCheck(SMALLBANKTxnType txn_type)
{
    txn_type_ = txn_type;
}


WriteCheck::~WriteCheck()
{
}


void WriteCheck::GenInputData()
{

}


void WriteCheck::GenInputData(ShardID shard_id)
{
    ThreadID thread_id = txn_identifier_->thread_id_;

    is_local_txn_   = true;
    local_shard_id_ = shard_id;
    
    if ((UtilFunc::Rand(100, thread_id) / (double)100.0) < g_smallbank_skewness)
    {
        //average access hot account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard * g_smallbank_hot_account, thread_id);
    }
    else
    {
        //average access all account
        a_name_ = UtilFunc::URand(1, g_smallbank_account_num_per_shard, thread_id);
    }

    value_ = UtilFunc::Rand(5, thread_id);
}


RC WriteCheck::RunTxn()
{
    uint64_t a_customer_id = 0;
    double   s_balance = 0.0;
    double   c_balance = 0.0;
    
    RC rc = RC_COMMIT;

    Index*      index = nullptr;
    void*       index_attr[5];
    IndexKey    index_key;
    uint64_t    index_and_opt_cnt = 0;
    IndexAndOpt index_and_opts[g_max_sec_indx_per_table];   //仅针对二级索引

    Tuple*      origin_tuple  = nullptr;
    Tuple*      operate_tuple = nullptr;
    Tuple*      new_tuple     = nullptr;
    TupleData   tuple_data    = nullptr;
    ColumnData  column_data   = nullptr;


    /*=======================================================================+
	EXEC SQL SELECT A_CUSTOMER_ID
		INTO :a_customer_id
		FROM ACCOUNT_T
		WHERE a_name =: a_name_;
	+========================================================================*/

    // printf("1\n");

    index = g_schema->GetIndex(A_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_name_;
    index_key = g_schema->GetIndexKey(A_PK_INDEX, index_attr);

    rc = index->IndexRead(index_key, origin_tuple);

    if (rc != RC_OK)
    {
        return RC_ABORT;
    }
    
    // printf("1.1\n");

    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, ACCOUNT_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);
    
    if (rc == RC_ABORT)
    {
        //abort transaction
        return RC::RC_ABORT;
    }

    // printf("1.2\n");
    tuple_data = operate_tuple->GetTupleData();
    g_schema->GetColumnValue(SMALLBANKTableType::ACCOUNT_T, AccountCol::A_CUSTOMER_ID, tuple_data, column_data);
    a_customer_id = *(uint64_t*)column_data;



    /*=======================================================================+
	EXEC SQL SELECT S_BALANCE
		INTO :s_balance
		FROM SAVING
		WHERE s_customer_id =: a_customer_id;
	+========================================================================*/

    // printf("2\n");

    index = g_schema->GetIndex(S_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(S_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::READ_AT, SMALLBANKTableType::SAVING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    
    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(SMALLBANKTableType::SAVING_T, SavingCol::S_BALANCE, tuple_data, column_data);
    s_balance = *(double*)column_data;



    /*=======================================================================+
    if  (s_balance + c_balance) < value
        EXEC SQL UPDATE checking SET c_balance =: c_balance - (value + 1)
   		WHERE c_customer_id =: a_customer_id;
    else
        EXEC SQL UPDATE checking SET c_balance =: c_balance - value
   		WHERE c_customer_id =: a_customer_id;    
	+========================================================================*/

    // printf("3\n");

    index = g_schema->GetIndex(C_PK_INDEX, local_shard_id_);
    index_attr[0] = &a_customer_id;
    index_key = g_schema->GetIndexKey(C_PK_INDEX, index_attr);
    
    rc = index->IndexRead(index_key, origin_tuple);
    if (rc != RC_OK)
        return RC::RC_ABORT;


    index_and_opt_cnt = 0;
    rc = txn_context_->AccessTuple(AccessType::UPDATE_AT, SMALLBANKTableType::CHECKING_T, local_shard_id_, index_and_opt_cnt, index_and_opts, origin_tuple, operate_tuple);

    if (rc == RC_ABORT)
        return RC::RC_ABORT;
    
    tuple_data = operate_tuple->GetTupleData();

    g_schema->GetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, column_data);
    c_balance = *(double*)column_data;

    if ((s_balance + c_balance) < value_) {
        c_balance -= (value_ + 1);
    } else {
        c_balance -= value_;
    }
    
    g_schema->SetColumnValue(SMALLBANKTableType::CHECKING_T, CheckingCol::C_BALANCE, tuple_data, (ColumnData)&c_balance);

    // printf("4\n");
    return RC::RC_COMMIT;

}



#endif
