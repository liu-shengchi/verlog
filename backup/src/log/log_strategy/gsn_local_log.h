#ifndef LOG_STRATEGY_GSN_LOCAL_LOG_H_
#define LOG_STRATEGY_GSN_LOCAL_LOG__H_

#include "config.h"

#include "log_strategy.h"

#if   LOG_STRATEGY_TYPE == GSN_LOCAL_LOG

class LogReplayContext;



class GSNLocalLog : public LogStrategy
{
private:
    
public:
    GSNLocalLog();
    ~GSNLocalLog();

    void DeconstructTxnLog(LogReplayContext* log_replay_context);

};


#endif



#endif
