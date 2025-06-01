#include "tpcc_util.h"


#if   WORKLOAD_TYPE == TPCC_W


/******** TPCCUtilFunc ********/

uint64_t TPCCUtilFunc::C_C_LAST_255 = 0; 
uint64_t TPCCUtilFunc::C_C_ID_1023  = 0; 
uint64_t TPCCUtilFunc::C_I_ID_8191  = 0;

const char TPCCUtilFunc::char_list_[61] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                                           'd','e','f','g','h','i','j','k','l','m','n','o',
                                           'p','q','r','s','t','u','v','w','x','y','z','A',
                                           'B','C','D','E','F','G','H','I','J','K','L','M',
                                           'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};

const char* TPCCUtilFunc::last_name_list_[10] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
                                                 "ESE", "ANTI", "CALLY", "ATION", "EING"};


#endif