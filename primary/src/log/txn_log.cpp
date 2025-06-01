#include "txn_log.h"

#include <string.h>
#include <malloc.h>



TxnLog::TxnLog(uint64_t buffer_size)
        : buffer_size_(buffer_size)
{   
    log_offset_ = 0;
    txn_log_buffer_ = (char*)malloc(buffer_size_);
    memset(txn_log_buffer_, 0, buffer_size_);    
}

TxnLog::~TxnLog()
{
    free(txn_log_buffer_);
}

