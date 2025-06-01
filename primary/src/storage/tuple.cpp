#include "tuple.h"

#include "s2pl_waitdie.h"
#include "silo.h"
#include "mvto.h"

#include "serial_log.h"
#include "taurus_log.h"
#include "logindex_log.h"

#include <malloc.h>
#include <string.h>

using namespace std;



/********* Tuple **********/

Tuple::Tuple()
{

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    cc_tuple_meta_ = new S2plWDTupleMeta();
#elif CC_STRATEGY_TYPE == SILO_CC
    cc_tuple_meta_ = new SiloTupleMeta();
#elif CC_STRATEGY_TYPE == MVTO_CC
    cc_tuple_meta_ = new MvtoTupleMeta();
#endif

#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    log_tuple_meta_ = new SerialTupleMeta();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    log_tuple_meta_ = new TaurusTupleMeta();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    log_tuple_meta_ = new LogIndexTupleMeta();
#endif

    tuple_data_    = nullptr;
}


Tuple::Tuple(uint64_t  tuple_size)
{

#if   CC_STRATEGY_TYPE == S2PL_WAITDIE_CC
    cc_tuple_meta_ = new S2plWDTupleMeta();
#elif CC_STRATEGY_TYPE == SILO_CC
    cc_tuple_meta_ = new SiloTupleMeta();
#elif CC_STRATEGY_TYPE == MVTO_CC
    cc_tuple_meta_ = new MvtoTupleMeta();
#endif

#if   LOG_STRATEGY_TYPE == SERIAL_LOG
    log_tuple_meta_ = new SerialTupleMeta();
#elif LOG_STRATEGY_TYPE == TAURUS_LOG
    log_tuple_meta_ = new TaurusTupleMeta();
#elif LOG_STRATEGY_TYPE == LOGINDEX_LOG
    log_tuple_meta_ = new LogIndexTupleMeta();
#endif
    
    tuple_data_ = (TupleData)malloc(tuple_size * sizeof(char));
    memset(tuple_data_, '0', tuple_size);
}


Tuple::~Tuple()
{
    delete cc_tuple_meta_;
    delete log_tuple_meta_;
    free(tuple_data_);
}


void Tuple::CopyTupleData(Tuple* source_tuple, uint64_t size)
{
    memcpy(tuple_data_, source_tuple->tuple_data_, size);
}

void Tuple::CopyTupleMeta(Tuple* source_tuple)
{
    cc_tuple_meta_->CopyCCTupleMeta(source_tuple->cc_tuple_meta_);
    log_tuple_meta_->CopyLogTupleMeta(source_tuple->log_tuple_meta_);
}


void Tuple::CopyLogTupleMeta(LogTupleMeta* log_tuple_meta)
{
    log_tuple_meta_->CopyLogTupleMeta(log_tuple_meta);
}