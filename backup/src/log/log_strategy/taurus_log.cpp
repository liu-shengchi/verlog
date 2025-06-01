#include "taurus_log.h"

#include "log_buffer.h"

#include "log_replay_context.h"


#include "string.h"


#if   LOG_STRATEGY_TYPE == TAURUS_LOG

/*********************************************/
/***************** TaurusLog *****************/
/*********************************************/
TaurusLog::TaurusLog()
{
}

TaurusLog::~TaurusLog()
{
}


void TaurusLog::DeconstructTxnLog(LogReplayContext* log_replay_context)
{
    
}


void TaurusLog::ReplayTxnLog(LogReplayContext* log_replay_context)
{

}



/*********************************************/
/************* TaurusLogTxnMeta **************/
/*********************************************/

TaurusLogTxnMeta::TaurusLogTxnMeta()
{
}

TaurusLogTxnMeta::~TaurusLogTxnMeta()
{
}



/*********************************************/
/************* TaurusLogTxnMeta **************/
/*********************************************/

TaurusLogTupleMeta::TaurusLogTupleMeta()
{
}

TaurusLogTupleMeta::~TaurusLogTupleMeta()
{
}


#endif