#include "access_entry.h"

/***** IndexAndOpt ****/
IndexAndOpt::IndexAndOpt()
{
    index_id_  = 0;
    index_opt_ = AccessType::READ_AT;
}

/***** AccessEntry ****/
AccessEntry::AccessEntry()
{
    access_type_ = AccessType::READ_AT;
    table_id_    = 0;
    shard_id_    = 0;
    index_and_opt_cnt_  = 0;
    access_entry_index_ = 0;
    
    origin_tuple_  = nullptr;
    operate_tuple_ = nullptr;

    modified_attr_bits_ = 0;
}