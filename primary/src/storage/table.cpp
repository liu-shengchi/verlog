#include "table.h"

// #include "global.h"

// #include "tuple.h"
// #include "index.h"





// Table::Table()
// {
// }


// Table::Table(TableID table_id, bool has_inc_id = false)
// {
//     table_id_   = table_id;
//     tuple_count_ = 0;

//     has_inc_id_ = has_inc_id;
//     inc_id_ = 0;
// }


// Table::Table(TableID table_id, Index* primary_key_index, map<IndexID, Index*>* second_indexes, 
//                 bool has_primary_key = true, 
//                 bool has_second_index = false, 
//                 bool has_inc_id = false)
// {
//     table_id_ = table_id;
//     primary_key_index_ =primary_key_index;
//     second_indexes_ = second_indexes;

//     has_primary_key_ = has_primary_key;
//     has_second_index_ = has_second_index;  

//     has_inc_id_ = has_inc_id;
//     inc_id_ = 0;

//     tuple_count_ = 0;
// }

// Table::~Table()
// {
// }


// RC Table::ReadTupleByPK(IndexKey index_key, ItemPtr* &item_ptr)
// {
//     RC rc = primary_key_index_->IndexRead(index_key, item_ptr);
//     return rc;
// }


// RC Table::InsertTuple(Tuple* tuple)
// {
//     RC rc = RC_OK;

//     return rc;
// }


// uint64_t Table::GetIncID()
// { 
//     return ATOM_FETCH_ADD(inc_id_, 1);
// }

// TableID Table::GetTableID() 
// { return table_id_; }
