#ifndef LOG_STRATEGY_SERIAL_H_
#define LOG_STRATEGY_SERIAL_H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == SERIAL_LOG

class LogReplayContext;



/*******************************************
串行日志
*******************************************/
class SerialLog : public LogStrategy
{
private:

public:
    SerialLog();
    ~SerialLog();

    void DeconstructTxnLog(LogReplayContext* log_replay_context);

    void ReplayTxnLog(LogReplayContext* log_replay_context);

    void FinishReplayTxn(LogReplayContext* log_replay_context);

};



class SerialLogTxnMeta : public LogTxnMeta
{
public:
    SerialLogTxnMeta();
    ~SerialLogTxnMeta();
};



class SerialLogTupleMeta : public LogTupleMeta
{
public:
    SerialLogTupleMeta();
    ~SerialLogTupleMeta();

};


#endif
#endif