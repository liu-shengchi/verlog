#ifndef LOG_STRATEGY_TAURUS_H_
#define LOG_STRATEGY_TAURUS_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == TAURUS_LOG


class LogReplayContext;




/*******************************************
Taurus并行日志
*******************************************/

class TaurusLog : public LogStrategy
{
private:
    
public:
    TaurusLog();
    ~TaurusLog();

    void DeconstructTxnLog(LogReplayContext* log_replay_context);

    void ReplayTxnLog(LogReplayContext* log_replay_context);

};



class TaurusLogTxnMeta : public LogTxnMeta
{
public:
    TaurusLogTxnMeta();
    ~TaurusLogTxnMeta();
};



class TaurusLogTupleMeta : public LogTupleMeta
{
private:
    
public:
    TaurusLogTupleMeta();
    ~TaurusLogTupleMeta();
};




#endif

#endif