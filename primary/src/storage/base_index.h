#ifndef STORAGE_BASE_INDEX_H_
#define STORAGE_BASE_INDEX_H_

#include "config.h"

#include <vector>

class Tuple;


class IndexBase
{
public:

    virtual RC          IndexInsert(IndexKey key, Tuple* tuple) = 0;

    virtual RC          IndexRead(IndexKey key, Tuple* &tuple) = 0;

    //for non-primary key index, return the set of tuples that equal to indexkey
    virtual RC          IndexRead(IndexKey key, Tuple** &tuples, uint64_t& tuple_count) = 0;

    virtual RC          IndexRemove(IndexKey key) = 0;

protected:

    //indicate which table the index belongs to
    TableID             table_id_;

    bool                is_pk_index_;

};


#endif