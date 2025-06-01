#ifndef STORAGE_COLUMN_H_
#define STORAGE_COLUMN_H_

#include "config.h"



class ColCatalog
{
public:
    TableID  table_id_;
    ColumnID column_id_;
    DataType data_type_;
    uint32_t col_size_;
    uint32_t col_offset_;
};



#endif
